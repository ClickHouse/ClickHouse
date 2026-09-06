#include <atomic>
#include <optional>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

/// Test what happens if the global pool runs out of threads while its queue still has room.
///
/// This is the shape of the real `GlobalThreadPool`, where `max_thread_pool_queue_size` (10000 by
/// default) is much larger than `max_thread_pool_size`, unlike in the `ThreadPool.GlobalFull*` tests
/// which give the global pool a queue as small as its thread count.
///
/// There was a bug: a `ThreadFromGlobalPool` scheduled when all the threads of the global pool are
/// already taken by other `ThreadFromGlobalPool`s was silently put into the queue. Its creator got a
/// seemingly running thread whose function was never called, and every wait for that thread to make
/// progress hung forever. With a small `max_thread_pool_size`, that starved the loader worker the
/// server waits for while starting up databases, so `clickhouse-server` hung at startup instead of
/// reporting that the setting is too small.

TEST(ThreadPool, GlobalFullLargeQueue)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    /// The queue is much larger than the number of threads, so a job that cannot get a thread would
    /// be accepted into the queue rather than rejected.
    global_pool.setQueueSize(1000);

    /// `ThreadFromGlobalPool`s of the local pools from the previous test cases may not have exited yet.
    global_pool.wait();

    std::atomic<size_t> started = 0;
    std::atomic<bool> release = false;

    /// Occupies its thread until `release` is set, like the never-returning workers of the background
    /// pools do for the whole lifetime of the server.
    auto occupy_thread = [&] { ++started; while (!release.load()) {} };

    {
        std::optional<ThreadPool> pool(
            std::in_place,
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
            capacity, capacity, capacity);

        for (size_t i = 0; i < capacity; ++i)
            pool->scheduleOrThrowOnError(occupy_thread);

        /// All the threads of the global pool are taken now.
        while (started != capacity)
            ;

        /// Direct permanent threads use the same fail-fast mode as the production startup workers.
        /// They must not be silently queued behind the never-returning jobs above.
        bool direct_thread_threw = false;
        try
        {
            ThreadFromGlobalPool direct_thread(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [] {});
        }
        catch (const DB::Exception &)
        {
            direct_thread_threw = true;
        }
        EXPECT_TRUE(direct_thread_threw) << "a direct permanent thread with no free global-pool worker must throw";

        /// There is no thread left for a new one and there never will be while the jobs above run, so
        /// this has to fail instead of waiting for a thread that is never going to be free.
        std::optional<ThreadPool> another_pool(
            std::in_place,
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
        bool threw = false;
        try
        {
            another_pool->scheduleOrThrowOnError([] {});
        }
        catch (const DB::Exception &)
        {
            threw = true;
        }
        EXPECT_TRUE(threw) << "scheduling with no free thread in the global pool must throw";

        release = true;
        pool->wait();

        if (!threw)
        {
            /// The unfixed pool silently put `another_pool`'s worker into the global queue instead of
            /// throwing. Destroying `another_pool` now would join a "thread" whose function never
            /// started — the very hang this test covers — so free the global slots first, letting the
            /// queued worker run and exit, and skip the rest of the test.
            pool.reset();
            another_pool.reset();
            global_pool.setMaxThreads(10000);
            global_pool.setMaxFreeThreads(1000);
            global_pool.setQueueSize(10000);
            return;
        }

        another_pool.reset();
        pool.reset();
    }

    /// The threads have exited, so their slots in the global pool are free again.
    global_pool.wait();

    std::atomic<size_t> counter = 0;
    {
        ThreadPool pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, capacity);
        for (size_t i = 0; i < capacity; ++i)
            pool.scheduleOrThrowOnError([&] { ++counter; });
        pool.wait();
    }
    EXPECT_EQ(counter, capacity);

    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}
