//! Regression coverage for constructing cancellation errors after their observers are gone.
#![cfg(all(feature = "client", feature = "http1"))]

mod support;

use bytes::Bytes;
use http_body_util::Empty;
use hyper::{body::Body, client::conn::http1, Request};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use support::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Meter;

thread_local! {
    static OBSERVING: Cell<bool> = Cell::new(false);
    static ALLOCATIONS: Cell<usize> = Cell::new(0);
    static BYTES: Cell<usize> = Cell::new(0);
}

fn record_allocation(bytes: usize) {
    if OBSERVING.try_with(Cell::get).unwrap_or(false) {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        BYTES.with(|count| count.set(count.get() + bytes));
    }
}

unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation(size);
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static METER: Meter = Meter;

fn count_allocations(f: impl FnOnce()) -> (usize, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    BYTES.with(|count| count.set(0));
    OBSERVING.with(|enabled| enabled.set(true));
    f();
    OBSERVING.with(|enabled| enabled.set(false));
    (ALLOCATIONS.with(Cell::get), BYTES.with(Cell::get))
}

fn assert_no_allocations(label: &str, f: impl FnOnce()) {
    let (allocations, bytes) = count_allocations(f);
    assert_eq!((allocations, bytes), (0, 0), "{label}");
}

/// A peer that stops making progress after a fixed response prefix.
struct SilentIo {
    response: &'static [u8],
    offset: usize,
    wrote: bool,
}

impl AsyncRead for SilentIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.as_mut().get_mut();
        if !this.wrote || this.offset == this.response.len() {
            return Poll::Pending;
        }
        let n = buf.remaining().min(this.response.len() - this.offset);
        buf.put_slice(&this.response[this.offset..this.offset + n]);
        this.offset += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for SilentIo {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        self.as_mut().get_mut().wrote = true;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

type Connection = http1::Connection<TokioIo<SilentIo>, Empty<Bytes>>;

fn connection(response: &'static [u8]) -> (http1::SendRequest<Empty<Bytes>>, Connection) {
    let io = SilentIo {
        response,
        offset: 0,
        wrote: false,
    };
    let mut handshake = Box::pin(http1::Builder::new().handshake::<_, Empty<Bytes>>(TokioIo::new(io)));
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match handshake.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(pair)) => pair,
        other => panic!("local HTTP/1 handshake did not complete: {other:?}"),
    }
}

fn request() -> Request<Empty<Bytes>> {
    Request::builder()
        .uri("/value")
        .header("host", "localhost")
        .body(Empty::new())
        .unwrap()
}

#[test]
fn unobserved_queued_and_dispatched_requests_do_not_allocate_on_drop() {
    let (mut sender, conn) = connection(b"");
    let response = sender.send_request(request());
    assert_no_allocations("queued request", || {
        drop(response);
        drop(sender);
        drop(conn);
    });

    let (mut sender, conn) = connection(b"");
    let response = sender.try_send_request(request());
    assert_no_allocations("queued retry request", || {
        drop(response);
        drop(sender);
        drop(conn);
    });

    let (mut sender, conn) = connection(b"");
    let response = sender.send_request(request());
    let mut conn = Box::pin(conn);
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(conn.as_mut().poll(&mut cx).is_pending());
    assert_no_allocations("dispatched request", || {
        drop(response);
        drop(sender);
        drop(conn);
    });

    let (mut sender, conn) = connection(b"");
    let response = sender.try_send_request(request());
    let mut conn = Box::pin(conn);
    assert!(conn.as_mut().poll(&mut cx).is_pending());
    assert_no_allocations("dispatched retry request", || {
        drop(response);
        drop(sender);
        drop(conn);
    });
}

#[test]
fn unobserved_incomplete_body_does_not_allocate_on_drop() {
    let (mut sender, conn) = connection(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nx");
    let mut response = Box::pin(sender.send_request(request()));
    let mut conn = Box::pin(conn);
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(conn.as_mut().poll(&mut cx).is_pending());
    assert!(conn.as_mut().poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut cx) else {
        panic!("response head should be available");
    };
    let mut body = response.into_body();
    let frame = match Pin::new(&mut body).poll_frame(&mut cx) {
        Poll::Ready(Some(Ok(frame))) => frame,
        Poll::Pending => {
            assert!(conn.as_mut().poll(&mut cx).is_pending());
            let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx) else {
                panic!("body frame should be available");
            };
            frame
        }
        _ => panic!("body frame should be available"),
    };
    assert_eq!(frame.data_ref().unwrap().as_ref(), b"x");
    assert_no_allocations("incomplete body with already delivered frame", || {
        drop(frame);
        drop(body);
        drop(response);
        drop(sender);
        drop(conn);
    });
}

#[test]
fn live_response_observer_still_receives_cancellation_error() {
    let (mut sender, conn) = connection(b"");
    let mut response = Box::pin(sender.send_request(request()));
    drop(conn);
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let Poll::Ready(Err(err)) = response.as_mut().poll(&mut cx) else {
        panic!("live response observer must receive an error");
    };
    assert!(err.is_canceled());
    drop(sender);
}

#[test]
fn live_body_observer_still_receives_incomplete_error() {
    let (mut sender, conn) = connection(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nx");
    let mut response = Box::pin(sender.send_request(request()));
    let mut conn = Box::pin(conn);
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(conn.as_mut().poll(&mut cx).is_pending());
    assert!(conn.as_mut().poll(&mut cx).is_pending());
    let Poll::Ready(Ok(response)) = response.as_mut().poll(&mut cx) else {
        panic!("response head should be available");
    };
    let mut body = response.into_body();
    drop(conn);
    let first = Pin::new(&mut body).poll_frame(&mut cx);
    let terminal = match first {
        Poll::Ready(Some(Ok(frame))) => {
            assert_eq!(frame.into_data().unwrap(), b"x"[..]);
            Pin::new(&mut body).poll_frame(&mut cx)
        }
        other => other,
    };
    let Poll::Ready(Some(Err(err))) = terminal else {
        panic!("live body observer must receive an incomplete-message error");
    };
    assert!(err.is_incomplete_message());
    drop(sender);
}
