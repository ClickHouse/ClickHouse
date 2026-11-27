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

/// Test what happens if local ThreadPool cannot create a ThreadFromGlobalPool.
/// There was a bug: if local ThreadPool cannot allocate even a single thread,
///  the job will be scheduled but never get executed.


TEST(ThreadPool, GlobalFull1)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    global_pool.setQueueSize(capacity);
    global_pool.wait();

    std::atomic<size_t> counter = 0;
    static constexpr size_t num_jobs = capacity + 1;

    auto func = [&] { ++counter; while (counter != num_jobs) {} };

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_jobs);

    for (size_t i = 0; i < capacity; ++i)
        pool.scheduleOrThrowOnError(func);

    for (size_t i = capacity; i < num_jobs; ++i)
    {
        EXPECT_THROW(pool.scheduleOrThrowOnError(func), DB::Exception);
        ++counter;
    }

    pool.wait();
    EXPECT_EQ(counter, num_jobs);

    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}


TEST(ThreadPool, GlobalFull2)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    global_pool.setQueueSize(capacity);

    /// ThreadFromGlobalPool from local thread pools from previous test case have exited
    ///  but their threads from global_pool may not have finished (they still have to exit).
    /// If we will not wait here, we can get "Cannot schedule a task exception" earlier than we expect in this test.
    global_pool.wait();

    std::atomic<size_t> counter = 0;
    auto func = [&] { ++counter; while (counter != capacity + 1) {} };

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, capacity, 0, capacity);
    for (size_t i = 0; i < capacity; ++i)
        pool.scheduleOrThrowOnError(func);

    ThreadPool another_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
    EXPECT_THROW(another_pool.scheduleOrThrowOnError(func), DB::Exception);

    ++counter;

    pool.wait();

    global_pool.wait();

    for (size_t i = 0; i < capacity; ++i)
        another_pool.scheduleOrThrowOnError([&] { ++counter; });

    another_pool.wait();
    EXPECT_EQ(counter, capacity * 2 + 1);

    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}
