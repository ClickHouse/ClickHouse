#include <atomic>

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
        ThreadPool pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
            capacity, capacity, capacity);

        for (size_t i = 0; i < capacity; ++i)
            pool.scheduleOrThrowOnError(occupy_thread);

        /// All the threads of the global pool are taken now.
        while (started != capacity)
            ;

        /// There is no thread left for a new one and there never will be while the jobs above run, so
        /// this has to fail instead of waiting for a thread that is never going to be free.
        ThreadPool another_pool(
            CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
        EXPECT_THROW(another_pool.scheduleOrThrowOnError([] {}), DB::Exception);

        release = true;
        pool.wait();
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
