#include <atomic>
#include <limits>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/83273
/// Configuring an unreasonably large queue_size (e.g. via *_queue_size server settings)
/// used to throw std::length_error from jobs.reserve(). The queue_size is only a logical
/// limit; the pre-reservation must be bounded so it neither throws nor attempts a huge
/// allocation, while the pool keeps working.

TEST(ThreadPool, HugeQueueSizeInConstructor)
{
    const size_t huge_queue_size = std::numeric_limits<size_t>::max();

    ThreadPool pool(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        /* max_threads */ 2,
        /* max_free_threads */ 0,
        huge_queue_size);

    /// setMaxThreads also re-reserves the queue, so exercise that path too.
    EXPECT_NO_THROW(pool.setMaxThreads(4));

    std::atomic<int> counter{0};
    for (size_t i = 0; i < 100; ++i)
        pool.scheduleOrThrowOnError([&]{ ++counter; });
    pool.wait();

    EXPECT_EQ(counter, 100);
    EXPECT_EQ(pool.getQueueSize(), huge_queue_size);
}

TEST(ThreadPool, HugeQueueSizeViaSetQueueSize)
{
    ThreadPool pool(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        /* max_threads */ 2,
        /* max_free_threads */ 0,
        /* queue_size */ 10);

    /// This mirrors StaticThreadPool::reloadConfiguration() reacting to a huge *_queue_size setting.
    EXPECT_NO_THROW(pool.setQueueSize(std::numeric_limits<size_t>::max()));

    std::atomic<int> counter{0};
    for (size_t i = 0; i < 100; ++i)
        pool.scheduleOrThrowOnError([&]{ ++counter; });
    pool.wait();

    EXPECT_EQ(counter, 100);
}
