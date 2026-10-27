#include <atomic>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

/// Test what happens if all the threads of the global pool are busy with *ordinary* jobs (jobs that
/// return, unlike the `ThreadFromGlobalPool` workers of `ThreadPool.GlobalFullLargeQueue`) while its
/// queue still has room.
///
/// A permanent thread created with `ThreadFromGlobalPoolScheduleMode::FailIfNoWorker` must be rejected
/// in this state: its creator relies on it running, and one of the ordinary jobs may be the one waiting
/// for it. But the worker of a local `ThreadPool` must wait in the queue instead: the local pool keeps
/// its jobs queued until the worker starts, and some callers cannot handle a failure caused by such a
/// transient saturation - `AsyncLoader` treats a failure of a mandatory spawn of its worker as fatal.

TEST(ThreadPool, GlobalBusyWithOrdinaryJobs)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    /// Restore the global pool even if the test fails, otherwise the following test cases would run
    /// with a global pool of a few threads and fail (or hang) for no reason of their own.
    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    /// The queue is much larger than the number of threads, so a job that cannot get a thread would
    /// be accepted into the queue rather than rejected.
    global_pool.setQueueSize(1000);

    /// `ThreadFromGlobalPool`s of the local pools from the previous test cases may not have exited yet.
    global_pool.wait();

    std::atomic<size_t> started = 0;
    std::atomic<size_t> finished = 0;
    std::atomic<bool> release = false;

    /// An ordinary job: it returns, so it does not hold a worker slot for the whole lifetime of its
    /// worker, but it does keep the worker busy while it runs.
    auto occupy_thread = [&] { ++started; while (!release.load()) {} ++finished; };

    for (size_t i = 0; i < capacity; ++i)
        global_pool.scheduleOrThrow(occupy_thread);

    /// All the threads of the global pool are busy now.
    while (started != capacity)
        ;

    bool direct_thread_threw = false;
    try
    {
        ThreadFromGlobalPool direct_thread(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [] {});
    }
    catch (const DB::Exception &)
    {
        direct_thread_threw = true;
    }
    EXPECT_TRUE(direct_thread_threw) << "a permanent thread with no free global-pool worker must throw";

    /// This creates a `ThreadFromGlobalPool` for the worker of the local pool. It has a free slot, so
    /// it is queued and runs the job as soon as one of the ordinary jobs above finishes.
    std::atomic<bool> local_job_ran = false;
    bool threw = false;
    {
        ThreadPool another_pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
        try
        {
            another_pool.scheduleOrThrowOnError([&] { local_job_ran = true; });
        }
        catch (const DB::Exception &)
        {
            threw = true;
        }
        EXPECT_FALSE(threw) << "the worker of a local pool must wait for a global-pool thread busy with an ordinary job";
        EXPECT_FALSE(local_job_ran);

        release = true;
        another_pool.wait();
    }
    EXPECT_EQ(local_job_ran, !threw);

    while (finished != capacity)
        ;

    /// Now no job of this test is left in the global pool.
    global_pool.wait();
}

TEST(ThreadPool, GlobalThreadCanWaitBehindOrdinaryJobs)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    std::atomic<size_t> started = 0;
    std::atomic<size_t> finished = 0;
    std::atomic<bool> release = false;
    std::atomic<bool> thread_ran = false;

    auto occupy_thread = [&]
    {
        ++started;
        while (!release.load())
        {
        }
        ++finished;
    };

    for (size_t i = 0; i < capacity; ++i)
    {
        global_pool.scheduleOrThrow(occupy_thread);
    }

    while (started != capacity)
    {
    }

    ThreadFromGlobalPool thread([&] { thread_ran = true; });
    EXPECT_FALSE(thread_ran);

    release = true;
    while (finished != capacity)
    {
    }

    thread.join();
    EXPECT_TRUE(thread_ran);

    global_pool.wait();
    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}
