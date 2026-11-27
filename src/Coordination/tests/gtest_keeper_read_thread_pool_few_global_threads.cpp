#include "config.h"

#if USE_NURAFT

#include <atomic>
#include <vector>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperReadThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric KeeperReadThreadsActive;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 parallel_read_threads;
    extern const CoordinationSettingsUInt64 parallel_read_min_batch;
}

/// The workers of `KeeperReadThreadPool` never return until shutdown, and a batch waits for all of them. When
/// fewer threads of the global pool are left than `parallel_read_threads`, the workers that cannot get a thread
/// used to be queued in the global pool forever, and the batch waited for them forever. Now they are rejected,
/// and the batch runs with the workers that have started.
TEST(KeeperReadThreadPool, FewerGlobalThreadsThanReadThreads)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 4;
    static constexpr size_t permanent_threads = 2;

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    std::atomic<size_t> started = 0;
    std::atomic<bool> release = false;
    std::vector<ThreadFromGlobalPool> threads;
    SCOPE_EXIT({
        release = true;
        for (auto & thread : threads)
            thread.join();
    });
    for (size_t i = 0; i < permanent_threads; ++i)
    {
        threads.emplace_back(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&]
        {
            ++started;
            while (!release.load())
            {
            }
        });
    }
    while (started != permanent_threads)
        ;

    DB::CoordinationSettings settings;
    settings[DB::CoordinationSetting::parallel_read_threads] = 8;
    settings[DB::CoordinationSetting::parallel_read_min_batch] = 1;

    static constexpr size_t requests = 1000;
    std::atomic<size_t> processed = 0;

    DB::KeeperReadThreadPool read_pool;
    for (size_t batch = 0; batch < 3; ++batch)
    {
        read_pool.execute(requests, settings, [&](size_t begin, size_t end) { processed += end - begin; });
        EXPECT_EQ(processed, (batch + 1) * requests);
    }
    read_pool.shutdown();
}

/// The workers that could not be started are retried with the next call of `execute`, and that call may run
/// its requests inline (fewer than `parallel_read_min_batch`). A worker started then must not take the batch
/// that ran before it was started for a new one: that batch has already been joined, and `busy_threads` would
/// underflow (an exception in debug builds).
TEST(KeeperReadThreadPool, RetriedWorkersSkipFinishedBatches)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 4;
    static constexpr size_t permanent_threads = 2;

    SCOPE_EXIT({
        global_pool.setMaxThreads(10000);
        global_pool.setMaxFreeThreads(1000);
        global_pool.setQueueSize(10000);
    });

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(0);
    global_pool.setQueueSize(1000);
    global_pool.wait();

    std::atomic<size_t> started = 0;
    std::atomic<bool> release = false;
    std::vector<ThreadFromGlobalPool> threads;
    auto release_threads = [&]
    {
        release = true;
        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    };
    SCOPE_EXIT({ release_threads(); });
    for (size_t i = 0; i < permanent_threads; ++i)
    {
        threads.emplace_back(ThreadFromGlobalPoolScheduleMode::FailIfNoWorker, [&]
        {
            ++started;
            while (!release.load())
            {
            }
        });
    }
    while (started != permanent_threads)
        ;

    DB::CoordinationSettings settings;
    settings[DB::CoordinationSetting::parallel_read_threads] = capacity;
    settings[DB::CoordinationSetting::parallel_read_min_batch] = 1;

    static constexpr size_t requests = 1000;
    std::atomic<size_t> processed = 0;
    auto func = [&](size_t begin, size_t end) { processed += end - begin; };

    DB::KeeperReadThreadPool read_pool;

    /// Only `capacity - permanent_threads` workers can start.
    read_pool.execute(requests, settings, func);
    EXPECT_EQ(processed, requests);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::KeeperReadThreadsActive), capacity - permanent_threads);

    /// Give the threads back, and run batches inline: the rest of the workers are started. A thread of the global
    /// pool gives its slot back a moment after it is joined, so starting a worker may need a few attempts.
    release_threads();
    settings[DB::CoordinationSetting::parallel_read_min_batch] = requests;
    size_t inline_batches = 0;
    while (CurrentMetrics::get(CurrentMetrics::KeeperReadThreadsActive) != static_cast<CurrentMetrics::Value>(capacity))
    {
        read_pool.execute(1, settings, func);
        ++inline_batches;
    }
    EXPECT_EQ(processed, requests + inline_batches);

    /// Parallel batches with all the workers.
    settings[DB::CoordinationSetting::parallel_read_min_batch] = 1;
    for (size_t batch = 0; batch < 3; ++batch)
        read_pool.execute(requests, settings, func);
    EXPECT_EQ(processed, 4 * requests + inline_batches);

    read_pool.shutdown();
}

#endif
