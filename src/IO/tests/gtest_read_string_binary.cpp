#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
}

using namespace DB;

TEST(ReadStringBinary, RespectsMaxSizeLimit)
{
    WriteBufferFromOwnString write_buf;

    std::string payload(100, 'x');
    writeStringBinary(payload, write_buf);
    write_buf.finalize();

    ReadBufferFromMemory read_buf(write_buf.str().data(), write_buf.str().size());
    const size_t limit = 50;
    std::string result;
    EXPECT_THROW(readStringBinary(result, read_buf, limit), Exception);
}

TEST(ReadStringBinary, WithinLimitSucceeds)
{
    WriteBufferFromOwnString write_buf;

    std::string payload(50, 'x');
    writeStringBinary(payload, write_buf);
    write_buf.finalize();

    ReadBufferFromMemory read_buf(write_buf.str().data(), write_buf.str().size());
    const size_t limit = 100;
    std::string result;
    readStringBinary(result, read_buf, limit);
    EXPECT_EQ(result, payload);
}

TEST(ReadStringBinary, DefaultLimitIsLarge)
{
    WriteBufferFromOwnString write_buf;

    std::string payload(1000, 'x');
    writeStringBinary(payload, write_buf);
    write_buf.finalize();

    ReadBufferFromMemory read_buf(write_buf.str().data(), write_buf.str().size());
    std::string result;
    readStringBinary(result, read_buf);
    EXPECT_EQ(result, payload);
}
