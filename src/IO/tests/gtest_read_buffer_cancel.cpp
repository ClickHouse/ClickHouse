#include <gtest/gtest.h>

#include <string>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>

/// Regression test for the "ReadBuffer is canceled" crash (STID 2508-2913).
///
/// Bug: TCPHandler::runImpl() idle loop called in->eof() on a ReadBuffer that
/// had been canceled by a previous query's pipeline callback. In debug/sanitizer
/// builds, ReadBuffer::next() has chassert(!isCanceled()) which aborts the
/// server process.
///
/// Fix: Added ReadBuffer::eofOrCanceled() which checks isCanceled() BEFORE
/// calling eof(). TCPHandler now uses eofOrCanceled() in the idle loop.

using namespace DB;

/// This test calls the PRODUCTION method ReadBuffer::eofOrCanceled() on a
/// canceled buffer. Without the isCanceled() check inside eofOrCanceled(),
/// eof() would be called on the canceled buffer, triggering
/// chassert(!isCanceled()) → abort → test FAILS (crash).
///
/// With the fix (isCanceled() short-circuits before eof()), the test PASSES.
TEST(ReadBufferCancelTest, EofOrCanceledOnCanceledBuffer)
{
    std::string data = "some data in the buffer";
    ReadBufferFromString buf(data);

    /// Consume all pending data so hasPendingData() returns false.
    /// This simulates a TCP ReadBuffer where all received bytes have been
    /// processed and eof() would need to call next() to check for more.
    buf.position() = buf.buffer().end();

    /// Simulate what happens when a callback's read from the TCP buffer
    /// fails: ReadBuffer::next() catches the exception from nextImpl()
    /// and calls cancel(), setting canceled = true.
    buf.cancel();

    /// This is the exact method called by TCPHandler::runImpl() idle loop.
    /// If eofOrCanceled() doesn't check isCanceled() first, this crashes
    /// with "ReadBuffer is canceled" in debug/sanitizer builds.
    EXPECT_TRUE(buf.eofOrCanceled());
}

/// Verify eofOrCanceled() behaves like eof() on a non-canceled buffer.
TEST(ReadBufferCancelTest, EofOrCanceledOnNormalBuffer)
{
    std::string data = "test data";
    ReadBufferFromString buf(data);

    /// Buffer has pending data — not at EOF
    EXPECT_FALSE(buf.eofOrCanceled());

    /// Consume all data
    buf.position() = buf.buffer().end();

    /// Now at EOF (no more data, not canceled)
    EXPECT_TRUE(buf.eofOrCanceled());
}

/// Verify that cancel() is persistent and isCanceled() reflects the state.
TEST(ReadBufferCancelTest, CancelIsPersistent)
{
    std::string data = "test";
    ReadBufferFromString buf(data);

    EXPECT_FALSE(buf.isCanceled());
    buf.cancel();
    EXPECT_TRUE(buf.isCanceled());

    /// Cancel is permanent — by design, a canceled TCP connection cannot be reused
    EXPECT_TRUE(buf.isCanceled());
}
