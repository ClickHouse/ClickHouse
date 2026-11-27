#include "config.h"

#if USE_NURAFT

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperReadThreadPool.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace CurrentMetrics
{
    extern const Metric KeeperReadThreadsActive;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 parallel_read_threads;
    extern const CoordinationSettingsUInt64 parallel_read_chunk_size;
    extern const CoordinationSettingsUInt64 parallel_read_min_batch;
}

/// Workers added by a reload of `parallel_read_threads` on a batch that runs inline (fewer requests than
/// `parallel_read_min_batch`) must wait for the next batch, not take the finished one for a new one.
TEST(KeeperReadThreadPool, ThreadsIncreasedOnInlineBatch)
{
    using namespace std::chrono_literals;

    DB::CoordinationSettings settings;
    settings[DB::CoordinationSetting::parallel_read_threads] = 2;
    settings[DB::CoordinationSetting::parallel_read_chunk_size] = 1;
    settings[DB::CoordinationSetting::parallel_read_min_batch] = 10;

    std::atomic<size_t> processed = 0;
    auto func = [&](size_t begin, size_t end)
    {
        if (begin == 0)
            std::this_thread::sleep_for(5ms);
        processed += end - begin;
    };

    const auto active_before = CurrentMetrics::get(CurrentMetrics::KeeperReadThreadsActive);

    DB::KeeperReadThreadPool pool;
    pool.execute(16, settings, func);
    EXPECT_EQ(processed, 16);

    settings[DB::CoordinationSetting::parallel_read_threads] = 6;
    pool.execute(1, settings, func);
    EXPECT_EQ(processed, 17);

    /// Let the added workers start.
    while (CurrentMetrics::get(CurrentMetrics::KeeperReadThreadsActive) < active_before + 6)
        std::this_thread::sleep_for(1ms);
    std::this_thread::sleep_for(50ms);

    pool.execute(24, settings, func);
    EXPECT_EQ(processed, 41);

    pool.shutdown();
}

#endif
