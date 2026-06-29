#include <atomic>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

TEST(ThreadPool, Loop)
{
    std::atomic<int> res{0};

    for (size_t i = 0; i < 1000; ++i)
    {
        size_t threads = 16;
        ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, threads);
        for (size_t j = 0; j < threads; ++j)
            pool.scheduleOrThrowOnError([&] { ++res; });
        pool.wait();
    }

    EXPECT_EQ(res, 16000);
}
