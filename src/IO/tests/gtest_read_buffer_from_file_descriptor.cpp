#include <gtest/gtest.h>

#include <base/types.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/AsynchronousReadBufferFromFile.h>
#include <IO/SynchronousReader.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

using namespace DB;

/// Test that `rewind` fully resets the buffer state, including the `canceled` flag.
/// This covers the scenario in `AsynchronousMetrics` where `/proc` files are read
/// in a loop: if a read fails and cancels the buffer, the next rewind+read
/// cycle must work without hitting the `chassert` in `ReadBuffer::next`.
TEST(ReadBufferFromFileDescriptor, RewindResetsBufferState)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        writeString(std::string_view{"hello world\n"}, out);
        out.finalize();
    }

    ReadBufferFromFile buf(tmp_file->path());

    /// First read should work.
    String line;
    readStringUntilEOF(line, buf);
    ASSERT_FALSE(line.empty());

    /// Simulate what happens when `nextImpl` throws: manually cancel the buffer.
    buf.cancel();
    ASSERT_TRUE(buf.isCanceled());

    /// `rewind` must fully reset the buffer.
    buf.rewind();

    ASSERT_FALSE(buf.isCanceled()) << "rewind() must reset the canceled flag";
    ASSERT_EQ(buf.getPosition(), 0) << "rewind() must reset file position to zero";
    ASSERT_EQ(buf.available(), 0) << "rewind() must clear the working buffer";

    /// Reading after rewind must return the same content.
    String line2;
    readStringUntilEOF(line2, buf);
    ASSERT_EQ(line, line2);
}

/// Same test for the asynchronous variant of the buffer.
TEST(AsynchronousReadBufferFromFileDescriptor, RewindResetsBufferState)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        writeString(std::string_view{"hello world\n"}, out);
        out.finalize();
    }

    SynchronousReader reader;
    AsynchronousReadBufferFromFileWithDescriptorsCache buf(reader, {}, tmp_file->path());

    /// First read should work.
    String line;
    readStringUntilEOF(line, buf);
    ASSERT_FALSE(line.empty());

    /// Simulate what happens when `nextImpl` throws: manually cancel the buffer.
    buf.cancel();
    ASSERT_TRUE(buf.isCanceled());

    /// `rewind` must fully reset the buffer.
    buf.rewind();

    ASSERT_FALSE(buf.isCanceled()) << "rewind() must reset the canceled flag";
    ASSERT_EQ(buf.getPosition(), 0) << "rewind() must reset file position to zero";
    ASSERT_EQ(buf.available(), 0) << "rewind() must clear the working buffer";

    /// Reading after rewind must return the same content.
    String line2;
    readStringUntilEOF(line2, buf);
    ASSERT_EQ(line, line2);
}
