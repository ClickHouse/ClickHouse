#include <atomic>
#include <thread>
#include <vector>

#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

/// Many threads wait in `scheduleImpl` for a slot of a full pool, and a finished job wakes only one of them.
/// Every job must still be scheduled and run.
TEST(ThreadPool, ManySchedulersOnFullPool)
{
    constexpr size_t num_schedulers = 32;
    constexpr size_t jobs_per_scheduler = 1000;

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        /* max_threads */ 2, /* max_free_threads */ 0, /* queue_size */ 2);

    /// The global pool is created lazily without synchronization, so create it before the schedulers start threads in it.
    GlobalThreadPool::instance();

    std::atomic<size_t> counter = 0;
    std::vector<std::thread> schedulers;
    for (size_t i = 0; i < num_schedulers; ++i)
    {
        schedulers.emplace_back([&]
        {
            for (size_t j = 0; j < jobs_per_scheduler; ++j)
                pool.scheduleOrThrowOnError([&] { ++counter; });
        });
    }

    for (auto & scheduler : schedulers)
        scheduler.join();
    pool.wait();

    EXPECT_EQ(counter, num_schedulers * jobs_per_scheduler);
}
