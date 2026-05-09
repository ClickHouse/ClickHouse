#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

TEST(ThreadPool, LIFOStartsManyBlockingJobs)
{
    constexpr size_t jobs = 64;
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, jobs, jobs, jobs);

    std::mutex mutex;
    std::condition_variable cv;
    size_t started = 0;
    bool release = false;

    for (size_t i = 0; i < jobs; ++i)
    {
        pool.scheduleOrThrowOnError([&]
        {
            std::unique_lock lock(mutex);
            ++started;
            cv.notify_all();
            cv.wait(lock, [&]
            {
                return release;
            });
        });
    }

    bool all_started = false;
    {
        std::unique_lock lock(mutex);
        all_started = cv.wait_for(lock, std::chrono::seconds(30), [&]
        {
            return started == jobs;
        });
        release = true;
    }
    cv.notify_all();

    pool.wait();

    EXPECT_TRUE(all_started) << "Only " << started << " jobs started";
}

TEST(ThreadPool, LIFONotifyWithThreadSelfRemoval)
{
    constexpr size_t jobs = 128;

    for (size_t iteration = 0; iteration < 100; ++iteration)
    {
        ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 8, 0, jobs);

        std::atomic<size_t> counter = 0;
        for (size_t i = 0; i < jobs; ++i)
        {
            pool.scheduleOrThrowOnError([&]
            {
                ++counter;
            });
        }

        pool.wait();
        EXPECT_EQ(counter.load(), jobs);
    }
}
