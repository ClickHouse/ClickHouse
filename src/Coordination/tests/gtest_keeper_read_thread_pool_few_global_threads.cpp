#include "config.h"

#if USE_NURAFT

#include <atomic>
#include <vector>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperReadThreadPool.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>

#include <gtest/gtest.h>

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

#endif
