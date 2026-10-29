#include <atomic>
#include <memory>
#include <vector>

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

    /// Each local pool creates a `ThreadFromGlobalPool` for its worker. There are more of them than the
    /// global pool has threads, but the workers return, so they take no slot, and the slots are not held
    /// by long-lived threads: all of them are queued and run their jobs as soon as the ordinary jobs
    /// above finish.
    static constexpr size_t local_pools_count = capacity + 2;
    std::atomic<size_t> local_jobs_ran = 0;
    bool threw = false;
    {
        std::vector<std::unique_ptr<ThreadPool>> local_pools;
        for (size_t i = 0; i < local_pools_count; ++i)
        {
            /// Like the pools of `AsyncLoader`, of query pipelines and many others: the jobs return. Some
            /// of these pools keep no free threads, others keep them; neither takes a slot.
            local_pools.push_back(std::make_unique<ThreadPool>(
                CurrentMetrics::LocalThread,
                CurrentMetrics::LocalThreadActive,
                CurrentMetrics::LocalThreadScheduled,
                /* max_threads = */ 1,
                /* max_free_threads = */ i % 2,
                /* queue_size = */ 0));
            try
            {
                local_pools.back()->scheduleOrThrowOnError([&] { ++local_jobs_ran; });
            }
            catch (const DB::Exception &)
            {
                threw = true;
            }
        }
        EXPECT_FALSE(threw) << "the worker of a local pool must wait for a global-pool thread busy with an ordinary job";
        EXPECT_EQ(local_jobs_ran, 0);

        release = true;
        for (auto & local_pool : local_pools)
            local_pool->wait();
    }
    if (!threw)
        EXPECT_EQ(local_jobs_ran, local_pools_count);

    while (finished != capacity)
        ;

    /// Now no job of this test is left in the global pool.
    global_pool.wait();
}

/// A worker that takes no slot of the global pool is accepted only while a slot is left. Once it is
/// accepted, a worker that takes the last slot and never returns must not overtake it in the queue,
/// otherwise the first one would never start.
TEST(ThreadPool, ReturningWorkerIsNotOvertakenByLongLivedWorker)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(1);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    std::atomic<bool> ordinary_started = false;
    std::atomic<bool> release_ordinary = false;
    global_pool.scheduleOrThrow([&] { ordinary_started = true; while (!release_ordinary.load()) {} });
    while (!ordinary_started)
        ;

    /// Its jobs return, so its worker takes no slot and waits in the queue behind the ordinary job.
    std::atomic<bool> returning_job_ran = false;
    ThreadPool returning_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0);
    returning_pool.scheduleOrThrowOnError([&] { returning_job_ran = true; });

    /// Its jobs are long-lived, so its worker takes the last slot, and it does not return until released.
    std::atomic<bool> long_lived_started = false;
    std::atomic<bool> release_long_lived = false;
    ThreadPool long_lived_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 1, /* queue_size = */ 0);
    long_lived_pool.setJobsAreLongLived();
    long_lived_pool.scheduleOrThrowOnError([&] { long_lived_started = true; while (!release_long_lived.load()) {} });

    release_ordinary = true;

    /// The returning worker was accepted first, so it runs first, while the long-lived one still waits.
    returning_pool.wait();
    EXPECT_TRUE(returning_job_ran);

    release_long_lived = true;
    long_lived_pool.wait();
    EXPECT_TRUE(long_lived_started);
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
