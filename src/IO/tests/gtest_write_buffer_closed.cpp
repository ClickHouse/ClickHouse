#include <gtest/gtest.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int CANNOT_WRITE_AFTER_BUFFER_CANCELED;
extern const int LOGICAL_ERROR;
}

namespace
{

int writeErrorCode(auto && write)
{
    try
    {
        write();
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

}

/// The inlined fast path of `WriteBuffer::write` checks only the `closed` flag, not `finalized` and `canceled`,
/// so it must reject every write to a canceled or finalized buffer even when there is free space left, including
/// writes of zero bytes, and `restart` must make the buffer writable again.
TEST(WriteBufferClosed, WriteAfterCancelThrows)
{
    WriteBufferFromOwnString out;
    out.write("abc", 3);
    out.cancel();
    ASSERT_GT(out.available(), 0);

    EXPECT_EQ(writeErrorCode([&] { out.write("x", 1); }), ErrorCodes::CANNOT_WRITE_AFTER_BUFFER_CANCELED);
    EXPECT_EQ(writeErrorCode([&] { out.write("", 0); }), ErrorCodes::CANNOT_WRITE_AFTER_BUFFER_CANCELED);

    /// A rollback of the cursor must not make a canceled buffer writable either.
    out.position() -= 3;
    EXPECT_EQ(writeErrorCode([&] { writeString("xyz", out); }), ErrorCodes::CANNOT_WRITE_AFTER_BUFFER_CANCELED);
}

TEST(WriteBufferClosed, RestartMakesBufferWritable)
{
    WriteBufferFromOwnString out;
    writeString("abc", out);
    out.finalize();
    EXPECT_EQ(out.str(), "abc");

    out.restart();
    writeString("de", out);
    writeChar('f', out);
    out.finalize();
    EXPECT_EQ(out.str(), "def");

    out.restart();
    writeString("gh", out);
    out.cancel();
    out.restart();
    writeString("ij", out);
    out.finalize();
    EXPECT_EQ(out.str(), "ij");
}

/// A write to a finalized buffer, and `write(char)` to a canceled one, throw `LOGICAL_ERROR`, which aborts in debug
/// and sanitizer builds, so these are checked in release builds only.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(WriteBufferClosed, WriteAfterFinalizeThrows)
{
    /// `WriteBufferFromPointer` keeps its memory on `finalize`, so there is free space left.
    char memory[16];
    WriteBufferFromPointer out(memory, sizeof(memory));
    out.write("abc", 3);
    out.finalize();
    ASSERT_GT(out.available(), 0);

    EXPECT_EQ(writeErrorCode([&] { out.write("x", 1); }), ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(writeErrorCode([&] { out.write("", 0); }), ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(writeErrorCode([&] { out.write('x'); }), ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(out.offset(), 3);
}

TEST(WriteBufferClosed, WriteCharAfterCancelThrows)
{
    WriteBufferFromOwnString out;
    out.write("abc", 3);
    out.cancel();
    ASSERT_GT(out.available(), 0);
    EXPECT_EQ(writeErrorCode([&] { out.write('x'); }), ErrorCodes::LOGICAL_ERROR);
}
#endif
