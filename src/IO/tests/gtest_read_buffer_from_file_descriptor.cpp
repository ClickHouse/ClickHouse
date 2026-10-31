#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <base/types.h>
#include <Common/filesystemHelpers.h>
#include <Common/Stopwatch.h>
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
    /// The retained suffix must not be counted twice.
    EXPECT_EQ(data.size(), out.count());

    String result(data.size(), '\0');
    ASSERT_EQ(4096, ::read(pipe_fds[0], result.data(), 4096));

    interrupted = false;
    out.finalize();
    EXPECT_EQ(data.size(), out.count());
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
    EXPECT_EQ(preserved.size(), out.count());

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
    EXPECT_EQ(preserved.size() + appended.size(), out.count());

    result.resize(appended.size());
    ASSERT_EQ(static_cast<ssize_t>(result.size()), ::read(pipe_fds[0], result.data(), result.size()));
    EXPECT_EQ(appended, result);

    ASSERT_EQ(0, ::close(pipe_fds[0]));
    ASSERT_EQ(0, ::close(pipe_fds[1]));
}

TEST(WriteBufferFromFileDescriptor, AppendsPromptlyToStuckSinkAfterInterruption)
{
    std::array<int, 2> pipe_fds{};
    ASSERT_EQ(0, ::pipe(pipe_fds.data()));
    ASSERT_EQ(4096, ::fcntl(pipe_fds[1], F_SETPIPE_SZ, 4096));

    const String filler(4096, 'f');
    ASSERT_EQ(static_cast<ssize_t>(filler.size()), ::write(pipe_fds[1], filler.data(), filler.size()));

    /// The reader stays paused while much more data than the buffer holds is appended. Every
    /// flush waits for one poll() timeout and retains the data, so this must not degrade into
    /// one timeout per few appended bytes.
    WriteBufferFromFileDescriptor out(pipe_fds[1], 4096);
    out.setCancellationHook([] { return false; }, [] { return true; });

    const String data(64 * 1024, 'd');
    Stopwatch watch;
    writeString(data, out);
    out.next();
    EXPECT_LT(watch.elapsedMilliseconds(), 10000);
    EXPECT_EQ(data.size(), out.count());

    /// Drain the pipe concurrently while finalizing and check that nothing was lost.
    out.setCancellationHook({});
    String result;
    std::thread reader([&]
    {
        std::array<char, 4096> chunk{};
        ssize_t res = 0;
        while ((res = ::read(pipe_fds[0], chunk.data(), chunk.size())) > 0)
            result.append(chunk.data(), res);
    });
    out.finalize();
    ASSERT_EQ(0, ::close(pipe_fds[1]));
    reader.join();
    EXPECT_EQ(filler + data, result);

    ASSERT_EQ(0, ::close(pipe_fds[0]));
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
    EXPECT_EQ(preserved.size(), fork.count());

    String drained(filler.size(), '\0');
    ASSERT_EQ(static_cast<ssize_t>(drained.size()), ::read(pipe_fds[0], drained.data(), drained.size()));

    interrupted = false;
    const String appended = " plus new data";
    writeString(appended, fork);
    fork.finalize();
    EXPECT_EQ(preserved.size() + appended.size(), fork.count());

    EXPECT_EQ(preserved + appended, mirrored);

    ASSERT_EQ(0, ::close(pipe_fds[0]));
    ASSERT_EQ(0, ::close(pipe_fds[1]));
}

/// Another writer of the same pipe (e.g. server logs written from another thread with `2>&1`)
/// can take the room reported by poll() before the buffer writes. The responsive path must not
/// then sleep in a blocking write(): once cancellation is requested and the reader stops, next()
/// has to return promptly. The shared descriptor must not be switched to non-blocking mode.
TEST(WriteBufferFromFileDescriptor, DoesNotBlockWhenAnotherWriterTakesRoom)
{
    std::array<int, 2> pipe_fds{};
    ASSERT_EQ(0, ::pipe(pipe_fds.data()));
    ASSERT_GT(::fcntl(pipe_fds[1], F_SETPIPE_SZ, 4096), 0);

    /// The competing writer uses its own non-blocking open file description of the same pipe and
    /// refills the pipe as soon as any room appears.
    const std::string proc_fd_path = "/proc/self/fd/" + std::to_string(pipe_fds[1]);
    const int competitor_fd = ::open(proc_fd_path.c_str(), O_WRONLY | O_CLOEXEC | O_NONBLOCK);
    ASSERT_GE(competitor_fd, 0);

    std::atomic<bool> stop_competitor = false;
    std::thread competitor([&]
    {
        const String chunk(4096, 'c');
        while (!stop_competitor)
        {
            pollfd poll_fd{.fd = competitor_fd, .events = POLLOUT, .revents = 0};
            if (::poll(&poll_fd, 1, 10) > 0)
            {
                [[maybe_unused]] ssize_t res = ::write(competitor_fd, chunk.data(), chunk.size());
            }
        }
    });

    std::atomic<bool> cancelled = false;
    std::atomic<bool> flushed = false;
    const String data(64 * 1024, 'd');
    WriteBufferFromFileDescriptor out(pipe_fds[1], data.size());
    out.setCancellationHook([&] { return cancelled.load(); });
    writeString(data, out);

    std::thread flusher([&]
    {
        out.next();
        flushed = true;
    });

    /// Let the reader free some room repeatedly, so that the buffer and the competitor race for it.
    std::array<char, 4096> drained{};
    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_GT(::read(pipe_fds[0], drained.data(), drained.size()), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    /// The reader stops: the pipe stays full from now on.
    cancelled = true;
    Stopwatch watch;
    while (!flushed && watch.elapsedMilliseconds() < 10000)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    const bool returned_promptly = flushed;

    /// Unblock the flusher if it is stuck in write(), so the test can finish.
    stop_competitor = true;
    competitor.join();
    while (!flushed)
    {
        pollfd poll_fd{.fd = pipe_fds[0], .events = POLLIN, .revents = 0};
        if (::poll(&poll_fd, 1, 10) > 0)
            EXPECT_GT(::read(pipe_fds[0], drained.data(), drained.size()), 0);
    }
    flusher.join();

    /// The interrupted `next` only discarded the pending bytes; the buffer itself is not cancelled yet.
    out.cancel();

    EXPECT_TRUE(returned_promptly);
    EXPECT_EQ(0, ::fcntl(pipe_fds[1], F_GETFL) & O_NONBLOCK);

    ASSERT_EQ(0, ::close(competitor_fd));
    ASSERT_EQ(0, ::close(pipe_fds[0]));
    ASSERT_EQ(0, ::close(pipe_fds[1]));
}
#endif
