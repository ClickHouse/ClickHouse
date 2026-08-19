#include <gtest/gtest.h>

#include <array>
#include <fcntl.h>
#include <unistd.h>

#include <base/types.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ForkWriteBuffer.h>
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

#if defined(OS_LINUX)
TEST(WriteBufferFromFileDescriptor, PreservesUnwrittenDataAfterInterruption)
{
    std::array<int, 2> pipe_fds{};
    ASSERT_EQ(0, ::pipe(pipe_fds.data()));

    /// One complete PIPE_BUF write succeeds, then the pipe is full and the interruption hook
    /// makes nextImpl() give control back to the caller before the remaining data is written.
    ASSERT_EQ(4096, ::fcntl(pipe_fds[1], F_SETPIPE_SZ, 4096));

    const String data(8192, 'x');
    bool interrupted = false;
    WriteBufferFromFileDescriptor out(pipe_fds[1], data.size());
    out.setCancellationHook([] { return false; }, [&] { return interrupted; });
    writeString(data, out);

    interrupted = true;
    out.next();

    String result(data.size(), '\0');
    ASSERT_EQ(4096, ::read(pipe_fds[0], result.data(), 4096));

    interrupted = false;
    out.finalize();
    ASSERT_EQ(4096, ::read(pipe_fds[0], result.data() + 4096, 4096));
    EXPECT_EQ(data, result);

    ASSERT_EQ(0, ::close(pipe_fds[0]));
    ASSERT_EQ(0, ::close(pipe_fds[1]));
}

TEST(WriteBufferFromFileDescriptor, PreservesFullBufferAfterInterruption)
{
    std::array<int, 2> pipe_fds{};
    ASSERT_EQ(0, ::pipe(pipe_fds.data()));
    ASSERT_EQ(4096, ::fcntl(pipe_fds[1], F_SETPIPE_SZ, 4096));

    const String filler(4096, 'f');
    ASSERT_EQ(static_cast<ssize_t>(filler.size()), ::write(pipe_fds[1], filler.data(), filler.size()));

    bool interrupted = true;
    const String preserved(4096, 'p');
    WriteBufferFromFileDescriptor out(pipe_fds[1], preserved.size());
    out.setCancellationHook([] { return false; }, [&] { return interrupted; });
    writeString(preserved, out);
    out.next();

    EXPECT_GT(out.available(), 0);

    String result(filler.size(), '\0');
    ASSERT_EQ(static_cast<ssize_t>(result.size()), ::read(pipe_fds[0], result.data(), result.size()));
    EXPECT_EQ(filler, result);

    interrupted = false;
    out.next();

    result.resize(preserved.size());
    ASSERT_EQ(static_cast<ssize_t>(result.size()), ::read(pipe_fds[0], result.data(), result.size()));
    EXPECT_EQ(preserved, result);

    const String appended = " appended";
    writeString(appended, out);
    out.finalize();

    result.resize(appended.size());
    ASSERT_EQ(static_cast<ssize_t>(result.size()), ::read(pipe_fds[0], result.data(), result.size()));
    EXPECT_EQ(appended, result);

    ASSERT_EQ(0, ::close(pipe_fds[0]));
    ASSERT_EQ(0, ::close(pipe_fds[1]));
}

TEST(ForkWriteBuffer, DoesNotReplayPreservedPrimarySuffixToSecondary)
{
    std::array<int, 2> pipe_fds{};
    ASSERT_EQ(0, ::pipe(pipe_fds.data()));
    ASSERT_EQ(4096, ::fcntl(pipe_fds[1], F_SETPIPE_SZ, 4096));

    const String filler(4096, 'f');
    ASSERT_EQ(static_cast<ssize_t>(filler.size()), ::write(pipe_fds[1], filler.data(), filler.size()));

    bool interrupted = true;
    auto primary = std::make_shared<WriteBufferFromFileDescriptor>(pipe_fds[1], 4096);
    primary->setCancellationHook([] { return false; }, [&] { return interrupted; });

    String mirrored;
    auto secondary = std::make_shared<WriteBufferFromString>(mirrored);
    ForkWriteBuffer fork({primary, secondary});

    const String preserved = "preserved suffix";
    writeString(preserved, fork);
    fork.next();

    String drained(filler.size(), '\0');
    ASSERT_EQ(static_cast<ssize_t>(drained.size()), ::read(pipe_fds[0], drained.data(), drained.size()));

    interrupted = false;
    const String appended = " plus new data";
    writeString(appended, fork);
    fork.finalize();

    EXPECT_EQ(preserved + appended, mirrored);

    ASSERT_EQ(0, ::close(pipe_fds[0]));
    ASSERT_EQ(0, ::close(pipe_fds[1]));
}
#endif
