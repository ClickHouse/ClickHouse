#include <Storages/System/StorageSystemStackTrace.h>

#if defined(OS_LINUX)

#include <Common/ErrnoException.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/getThreadId.h>

#include <gtest/gtest.h>
#include <fmt/format.h>

#include <cerrno>
#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

/// A thread can exit after `system.stack_trace` opened one of its procfs files and before it read it.

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

using namespace DB;

namespace
{

/// A thread that stays alive until `exitAndWaitUntilGone` is called.
class TestThread
{
public:
    TestThread()
    {
        std::promise<UInt64> tid_promise;
        auto tid_future = tid_promise.get_future();
        thread = std::thread(
            [promise = std::move(tid_promise), released = release.get_future()]() mutable
            {
                promise.set_value(getThreadId());
                released.wait();
            });
        tid = tid_future.get();
    }

    ~TestThread()
    {
        if (thread.joinable())
        {
            release.set_value();
            thread.join();
        }
    }

    /// Returns false if the kernel still lists the thread after 30 seconds.
    bool exitAndWaitUntilGone()
    {
        release.set_value();
        thread.join();

        /// join() returns slightly before the kernel removes the task from procfs.
        const String task_dir = fmt::format("/proc/self/task/{}", tid);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::filesystem::exists(task_dir))
        {
            if (std::chrono::steady_clock::now() > deadline)
                return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    UInt64 tid = 0;

private:
    std::promise<void> release;
    std::thread thread;
};

String statusPath(UInt64 tid)
{
    return fmt::format("/proc/{}/status", tid);
}

String commPath(UInt64 tid)
{
    return fmt::format("/proc/self/task/{}/comm", tid);
}

using PathFormat = String (*)(UInt64);
const PathFormat path_formats[] = {statusPath, commPath};

}

TEST(StorageSystemStackTrace, ReadAfterThreadExit)
{
    for (auto path_format : path_formats)
    {
        TestThread thread;
        const String path = path_format(thread.tid);
        SCOPED_TRACE(path);

        ReadBufferFromFile file(path);
        ASSERT_TRUE(thread.exitAndWaitUntilGone());

        try
        {
            String content;
            readStringUntilEOF(content, file);
            FAIL() << "Reading the procfs file of an exited thread succeeded: " << content;
        }
        catch (const ErrnoException & e)
        {
            EXPECT_EQ(e.getErrno(), ESRCH);
            EXPECT_TRUE(isThreadExitedError(e));
        }
    }
}

TEST(StorageSystemStackTrace, OpenAfterThreadExit)
{
    for (auto path_format : path_formats)
    {
        TestThread thread;
        const String path = path_format(thread.tid);
        SCOPED_TRACE(path);

        ASSERT_TRUE(thread.exitAndWaitUntilGone());

        try
        {
            ReadBufferFromFile file(path);
            FAIL() << "Opening the procfs file of an exited thread succeeded";
        }
        catch (const ErrnoException & e)
        {
            EXPECT_EQ(e.getErrno(), ENOENT);
            EXPECT_TRUE(isThreadExitedError(e));
        }
    }
}

TEST(StorageSystemStackTrace, OtherErrnoIsNotThreadExit)
{
    EXPECT_FALSE(isThreadExitedError(ErrnoException("Cannot read from file", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, EIO)));
    EXPECT_FALSE(isThreadExitedError(ErrnoException("Cannot read from file", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, EACCES)));
}

#endif
