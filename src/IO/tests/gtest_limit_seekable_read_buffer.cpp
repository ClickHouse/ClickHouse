#include <gtest/gtest.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

using namespace DB;

/// Regression test for UBSAN finding STID 3079-3df0:
///   src/IO/LimitSeekableReadBuffer.cpp:97:34:
///   runtime error: applying non-zero offset 1178 to null pointer
///
/// Reproduction scenario: the inner buffer has not materialized its memory yet
/// (e.g. `ParallelReadBuffer`, `ReadWriteBufferFromHTTP`, `EmptyReadBuffer` —
/// all start with `pos == nullptr`). When `LimitSeekableReadBuffer` wraps such
/// a buffer and `seek(non_zero_offset, SEEK_SET)` is called before the first
/// read, the fast-path check in `LimitSeekableReadBuffer::seek` used to
/// compute `pos + position_change`. Doing pointer arithmetic with a non-zero
/// offset on a null pointer is undefined behavior per [expr.add] and UBSAN
/// catches it under `-fsanitize=pointer-overflow`.
TEST(LimitSeekableReadBuffer, SeekOnInnerBufferWithNullPos)
{
    /// EmptyReadBuffer is constructed via `SeekableReadBuffer(nullptr, 0)`, so
    /// its `pos` and `working_buffer` are both null pointers. This is the
    /// cheapest way to reproduce the UB; real-world triggers from the backup
    /// stack (STID 3079-3df0) involve HTTP or parallel read buffers that also
    /// start with `pos == nullptr` until the first `next()` call.
    auto inner = std::make_unique<EmptyReadBuffer>();
    LimitSeekableReadBuffer limit_buf(std::move(inner), 0, 4096);

    /// Before the fix this line produced:
    ///   runtime error: applying non-zero offset 1178 to null pointer
    /// under `-fsanitize=pointer-overflow`. After the fix, the fast-path uses
    /// integer arithmetic on pointer differences and is well-defined.
    off_t result = limit_buf.seek(1178, SEEK_SET);
    ASSERT_EQ(result, 1178);
}

/// Sanity check that seeks still work correctly after reading when the inner
/// buffer does have a materialized working buffer. This guards against the
/// fix accidentally breaking the normal in-buffer seek fast-path.
TEST(LimitSeekableReadBuffer, SeekInsideMaterializedBuffer)
{
    const std::string data = "0123456789abcdef0123456789abcdef";
    auto inner = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
    LimitSeekableReadBuffer limit_buf(std::move(inner), 2, 20);

    /// Force the buffer to materialize.
    char c = 0;
    readChar(c, limit_buf);
    ASSERT_EQ(c, '2'); /// data[2]
    ASSERT_EQ(limit_buf.getPosition(), 1);

    /// Forward seek inside the working buffer — fast path.
    off_t result = limit_buf.seek(5, SEEK_SET);
    ASSERT_EQ(result, 5);
    readChar(c, limit_buf);
    ASSERT_EQ(c, '7'); /// data[2 + 5]

    /// Backward seek inside the working buffer — fast path.
    result = limit_buf.seek(0, SEEK_SET);
    ASSERT_EQ(result, 0);
    readChar(c, limit_buf);
    ASSERT_EQ(c, '2'); /// data[2]
}

/// Sanity check for the slow-path where the seek target is outside the
/// currently-buffered range. Ensures we don't accidentally take the fast
/// path when the target is outside the working buffer.
TEST(LimitSeekableReadBuffer, SeekOutsideMaterializedBuffer)
{
    const std::string data(1024, 'x');
    auto inner = std::make_unique<ReadBufferFromMemory>(data.data(), data.size());
    LimitSeekableReadBuffer limit_buf(std::move(inner), 0, data.size());

    char c = 0;
    readChar(c, limit_buf);
    ASSERT_EQ(c, 'x');

    /// Seek far forward — outside any reasonable working buffer range.
    off_t result = limit_buf.seek(512, SEEK_SET);
    ASSERT_EQ(result, 512);
    readChar(c, limit_buf);
    ASSERT_EQ(c, 'x');
}
