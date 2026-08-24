#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>

/** Reproduces bug in ThreadPool.
  * It get stuck if we call 'wait' many times from many other threads simultaneously.
  */


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

TEST(ThreadPool, ConcurrentWait)
{
    auto worker = []
    {
        for (size_t i = 0; i < 100000000; ++i)
            __asm__ volatile ("nop");
    };

    constexpr size_t num_threads = 4;
    constexpr size_t num_jobs = 4;

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);

    for (size_t i = 0; i < num_jobs; ++i)
        pool.scheduleOrThrowOnError(worker);

    constexpr size_t num_waiting_threads = 4;

    ThreadPool waiting_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_waiting_threads);

    for (size_t i = 0; i < num_waiting_threads; ++i)
        waiting_pool.scheduleOrThrowOnError([&pool] { pool.wait(); });

    waiting_pool.wait();
}
