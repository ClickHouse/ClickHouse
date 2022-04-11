#include <gtest/gtest.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

using namespace DB;

TEST(ReadBufferFromFile, seekBackwards)
{
    static constexpr size_t N = 256;
    static constexpr size_t BUF_SIZE = 64;

    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        for (size_t i = 0; i < N; ++i)
            writeIntBinary(i, out);
    }

    ReadBufferFromFile in(tmp_file->path(), BUF_SIZE);
    size_t x;

    /// Read something to initialize the buffer.
    in.seek(BUF_SIZE * 10, SEEK_SET);
    readIntBinary(x, in);

    /// Check 2 consecutive  seek calls without reading.
    in.seek(BUF_SIZE * 2, SEEK_SET);
    in.seek(BUF_SIZE, SEEK_SET);

    readIntBinary(x, in);
    ASSERT_EQ(x, 8);
}
