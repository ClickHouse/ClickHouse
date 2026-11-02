#include <atomic>
#include <chrono>
#include <thread>
#include <memory>
#include <vector>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

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

/// A worker of the global pool that holds no slot is not counted on to free up for a worker that it
/// schedules itself, because its job may wait for that worker: a table loader waits for the workers
/// that load its parts. When it is the only worker left besides the long-lived ones, such a nested
/// worker must be rejected instead of being queued behind it forever.
TEST(ThreadPool, NestedReturningWorkerOnLastFreeThreadIsRejected)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(2);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    /// Takes one of the two slots until released.
    std::atomic<bool> long_lived_started = false;
    std::atomic<bool> release_long_lived = false;
    ThreadPool long_lived_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 1, /* queue_size = */ 0);
    long_lived_pool.setJobsAreLongLived();
    long_lived_pool.scheduleOrThrowOnError([&] { long_lived_started = true; while (!release_long_lived.load()) {} });
    while (!long_lived_started)
        ;

    ThreadPool inner_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0);
    ThreadPool outer_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0);

    /// The worker of the outer pool takes the last thread of the global pool, so the worker of the inner
    /// pool could only run after the outer job, which would wait for it.
    std::atomic<int> error_code = 0;
    outer_pool.scheduleOrThrowOnError([&]
    {
        try
        {
            inner_pool.scheduleOrThrowOnError([] {});
        }
        catch (const DB::Exception & e)
        {
            error_code = e.code();
        }
    });
    outer_pool.wait();
    EXPECT_EQ(error_code, DB::ErrorCodes::CANNOT_SCHEDULE_TASK);

    release_long_lived = true;
    long_lived_pool.wait();
    inner_pool.wait();
}

/// An idle worker of a local pool that keeps free threads holds its thread of the global pool for an
/// unlimited time. When the other threads of the global pool are held by long-lived threads, it must leave
/// instead of waiting in the idle stack, otherwise a queued worker of another local pool would never start,
/// while it is accepted because a slot is left. With a remote metadata disk, the startup could hang this
/// way with a slightly too small `max_thread_pool_size`: the idle workers of the pools that read from the
/// disk could hold the threads that the workers parsing the metadata of the `system` database waited for.
TEST(ThreadPool, IdleLocalWorkerDoesNotHoldLastFreeThread)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 3;

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    /// Hold all the slots but one until released.
    std::atomic<size_t> long_lived_started = 0;
    std::atomic<bool> release_long_lived = false;
    std::vector<ThreadFromGlobalPool> long_lived_threads;
    for (size_t i = 0; i < capacity - 1; ++i)
    {
        long_lived_threads.emplace_back(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&]
        {
            ++long_lived_started;
            while (!release_long_lived.load())
            {
            }
        });
    }
    while (long_lived_started != capacity - 1)
        ;

    /// Its worker runs a finite job on the last free thread of the global pool, and would stay idle after it.
    ThreadPool pool_with_free_threads(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 1, /* queue_size = */ 0);
    pool_with_free_threads.scheduleOrThrowOnError([] {});
    pool_with_free_threads.wait();

    /// Its worker takes no slot and is queued until a thread of the global pool frees up.
    std::atomic<bool> job_ran = false;
    ThreadPool other_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0);
    other_pool.scheduleOrThrowOnError([&] { job_ran = true; });
    const bool finished_in_time = other_pool.waitUntil(std::chrono::steady_clock::now() + std::chrono::seconds(60));

    release_long_lived = true;
    for (auto & thread : long_lived_threads)
        thread.join();
    other_pool.wait();

    EXPECT_TRUE(finished_in_time) << "an idle worker of a local pool must not hold the last free thread of the global pool";
    EXPECT_TRUE(job_ran);
}

/// The other ordering: the worker of a local pool goes idle and takes a slot of the global pool first, and
/// then a long-lived thread takes the last slot. A worker of another local pool that takes no slot must not
/// be rejected then: the slot of the idle worker is taken back, the idle worker leaves its pool, and the new
/// worker runs on the thread it frees. A rejection here is fatal for the mandatory spawn of an `AsyncLoader`
/// worker. Then a long-lived thread that finds no slot left takes over the slot of an idle worker as well.
TEST(ThreadPool, IdleLocalWorkerGivesBackItsSlot)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 2;

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    /// Its worker runs a finite job, then stays idle and takes one of the two slots.
    ThreadPool pool_with_free_threads(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 1, /* queue_size = */ 0);
    pool_with_free_threads.scheduleOrThrowOnError([] {});
    pool_with_free_threads.wait();

    /// Takes the other thread of the global pool and the last slot.
    std::atomic<bool> long_lived_started = false;
    std::atomic<bool> release_long_lived = false;
    ThreadFromGlobalPool long_lived_thread(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&]
    {
        long_lived_started = true;
        while (!release_long_lived.load())
        {
        }
    });
    while (!long_lived_started)
        ;

    /// Its worker takes no slot. It must be accepted and run on the thread freed by the idle worker.
    std::atomic<bool> job_ran = false;
    ThreadPool other_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0);
    bool threw = false;
    try
    {
        other_pool.scheduleOrThrowOnError([&] { job_ran = true; });
    }
    catch (const DB::Exception &)
    {
        threw = true;
    }
    EXPECT_FALSE(threw) << "a worker that takes no slot must not be rejected while an idle worker holds a slot";
    const bool finished_in_time = threw || other_pool.waitUntil(std::chrono::steady_clock::now() + std::chrono::seconds(60));

    /// The worker of `pool_with_free_threads` goes idle again and takes the free slot (it is not the last one
    /// once `long_lived_thread` is gone, so release it first and start a new long-lived worker afterwards).
    release_long_lived = true;
    long_lived_thread.join();
    other_pool.wait();
    EXPECT_TRUE(finished_in_time) << "the idle worker must leave and free its thread of the global pool";
    EXPECT_TRUE(job_ran);

    pool_with_free_threads.scheduleOrThrowOnError([] {});
    pool_with_free_threads.wait();

    /// One slot is held by the idle worker, so the first long-lived worker takes the free one, and the second
    /// one takes over the slot of the idle worker and runs on the thread that the idle worker frees.
    std::atomic<size_t> long_lived_workers_started = 0;
    std::atomic<bool> release_long_lived_workers = false;
    ThreadPool long_lived_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ capacity, /* max_free_threads = */ 0, /* queue_size = */ 0);
    long_lived_pool.setJobsAreLongLived();
    bool long_lived_threw = false;
    try
    {
        for (size_t i = 0; i < capacity; ++i)
        {
            long_lived_pool.scheduleOrThrowOnError([&]
            {
                ++long_lived_workers_started;
                while (!release_long_lived_workers.load())
                {
                }
            });
        }
    }
    catch (const DB::Exception &)
    {
        long_lived_threw = true;
    }
    EXPECT_FALSE(long_lived_threw) << "a long-lived worker must take over the slot of an idle worker";

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!long_lived_threw && long_lived_workers_started != capacity && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_EQ(long_lived_workers_started, long_lived_threw ? long_lived_workers_started.load() : capacity);

    release_long_lived_workers = true;
    long_lived_pool.wait();
}

/// The slot of an evicted idle worker must be visible to the next scheduling call at once, not only when the
/// evicted worker wakes up and gives it back. Otherwise the second of two workers scheduled one after another
/// is rejected, which is fatal for the mandatory spawn of an `AsyncLoader` worker, although the thread freed
/// by the evicted worker is enough to run both of them one after another.
TEST(ThreadPool, SlotOfEvictedIdleWorkerIsVisibleAtOnce)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 2;
    static constexpr size_t num_other_pools = 4;

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    /// Its worker runs a finite job, then stays idle and takes one of the two slots.
    ThreadPool pool_with_free_threads(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 1, /* queue_size = */ 0);
    pool_with_free_threads.scheduleOrThrowOnError([] {});
    pool_with_free_threads.wait();

    /// Takes the other thread of the global pool and the last slot.
    std::atomic<bool> long_lived_started = false;
    std::atomic<bool> release_long_lived = false;
    ThreadFromGlobalPool long_lived_thread(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&]
    {
        long_lived_started = true;
        while (!release_long_lived.load())
        {
        }
    });
    while (!long_lived_started)
        ;

    /// Their workers take no slot. They are scheduled right one after another, likely before the evicted
    /// worker wakes up, and all of them must be accepted.
    std::atomic<size_t> jobs_ran = 0;
    std::vector<std::unique_ptr<ThreadPool>> other_pools;
    size_t rejected = 0;
    for (size_t i = 0; i < num_other_pools; ++i)
    {
        other_pools.push_back(std::make_unique<ThreadPool>(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
            /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0));
        try
        {
            other_pools.back()->scheduleOrThrowOnError([&] { ++jobs_ran; });
        }
        catch (const DB::Exception &)
        {
            ++rejected;
        }
    }
    EXPECT_EQ(rejected, 0) << "a worker that takes no slot must not be rejected while an evicted worker is waking up";

    bool finished_in_time = true;
    for (auto & pool : other_pools)
        finished_in_time = pool->waitUntil(std::chrono::steady_clock::now() + std::chrono::seconds(60)) && finished_in_time;

    release_long_lived = true;
    long_lived_thread.join();
    for (auto & pool : other_pools)
        pool->wait();

    EXPECT_TRUE(finished_in_time);
    EXPECT_EQ(jobs_ran, num_other_pools - rejected);
}

/// A worker of a local pool that finishes its job while a job waits in the queue of the global pool must not
/// go idle and keep its thread of the global pool: the queued job may be needed by the thread that scheduled
/// it, which counted on a thread that holds no slot to free up. Here the outer thread schedules the worker of
/// another local pool when two slots are left (enough for a nested wait), and waits for it. Only then the job
/// of `pool_with_free_threads` finishes, and its worker would take one of the two slots and wait in the idle
/// stack. Then the three threads of the global pool are held by the long-lived thread, by the waiting outer
/// thread and by the idle worker, and the queued worker would never start.
TEST(ThreadPool, IdleLocalWorkerLeavesWhileGlobalJobIsQueued)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 3;

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    /// Holds one slot until released.
    std::atomic<bool> long_lived_started = false;
    std::atomic<bool> release_long_lived = false;
    ThreadFromGlobalPool long_lived_thread(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&]
    {
        long_lived_started = true;
        while (!release_long_lived.load())
        {
        }
    });
    while (!long_lived_started)
        ;

    /// Its worker runs a job on the second thread until released, and would stay idle after it.
    std::atomic<bool> short_job_started = false;
    std::atomic<bool> release_short_job = false;
    ThreadPool pool_with_free_threads(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 1, /* queue_size = */ 0);
    pool_with_free_threads.scheduleOrThrowOnError([&]
    {
        short_job_started = true;
        while (!release_short_job.load())
        {
        }
    });
    while (!short_job_started)
        ;

    /// Runs on the third thread, takes no slot, and waits for the worker of `inner_pool`, which is queued.
    ThreadPool inner_pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads = */ 1, /* max_free_threads = */ 0, /* queue_size = */ 0);
    std::atomic<bool> inner_scheduled = false;
    std::atomic<bool> inner_ran = false;
    std::atomic<int> error_code = 0;
    ThreadFromGlobalPool outer_thread(ThreadFromGlobalPoolScheduleMode::ReturningFailIfNoWorker, [&]
    {
        try
        {
            inner_pool.scheduleOrThrowOnError([&] { inner_ran = true; });
        }
        catch (const DB::Exception & e)
        {
            error_code = e.code();
        }
        inner_scheduled = true;
        inner_pool.wait();
    });
    while (!inner_scheduled)
        ;
    EXPECT_EQ(error_code, 0);

    /// Now the worker of `pool_with_free_threads` finishes its job while the worker of `inner_pool` is queued.
    release_short_job = true;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (error_code == 0 && !inner_ran && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    const bool ran_in_time = inner_ran;

    /// If the idle worker did keep its thread, make it leave, so that the test does not hang.
    pool_with_free_threads.setMaxFreeThreads(0);
    outer_thread.join();
    release_long_lived = true;
    long_lived_thread.join();

    EXPECT_TRUE(ran_in_time) << "an idle worker of a local pool must not keep its thread while a job of the global pool is queued";
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
