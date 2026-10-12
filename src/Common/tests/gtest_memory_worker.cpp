#include <Common/Jemalloc.h>

/// Without jemalloc the worker can only measure a cgroup, whose other processes make
/// a before/after RSS comparison unsuitable for this test.
#if USE_JEMALLOC

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryWorker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <base/scope_guard.h>

namespace CurrentMetrics
{
    extern const Metric MemoryTrackingUnmeasured;
}

namespace ProfileEvents
{
    extern const Event MemoryWorkerRun;
}

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

bool waitForMemoryWorker()
{
    /// The first completed tick could have sampled the metric before it changed.
    const auto target = ProfileEvents::global_counters[ProfileEvents::MemoryWorkerRun] + 2;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (ProfileEvents::global_counters[ProfileEvents::MemoryWorkerRun] < target)
    {
        if (std::chrono::steady_clock::now() >= deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

TEST(MemoryWorker, UnmeasuredMemoryParticipatesInRSSWithoutTrackerCorrection)
{
    DB::ThreadStatus thread_status;
    thread_status.untracked_memory_limit = 0;

    const Int64 original_amount = total_memory_tracker.get();
    const Int64 original_rss = total_memory_tracker.getRSS();
    const Int64 original_limit = total_memory_tracker.getHardLimit();
    SCOPE_EXIT({
        total_memory_tracker.setHardLimit(original_limit);
        MemoryTracker::updateAllocated(original_amount, false);
        MemoryTracker::updateRSS(original_rss);
    });
    total_memory_tracker.setHardLimit(0);

    DB::MemoryWorkerConfig config;
    config.rss_update_period_ms = 1;
    config.use_cgroup = false;
    config.correct_tracker = false;
    config.rss_speculative_reserve_ratio = 0;
    auto worker = std::make_unique<DB::MemoryWorker>(config, nullptr);
    worker->start();
    ASSERT_TRUE(waitForMemoryWorker());
    const Int64 baseline_rss = total_memory_tracker.getRSS();

    /// Inject the same metric that holds shared-memory charges without allocating
    /// physical pages. Allow for ordinary allocations by the worker and test runner.
    constexpr Int64 charge = 1LL << 30;
    constexpr Int64 tolerance = 64LL << 20;
    CurrentMetrics::Increment unmeasured(CurrentMetrics::MemoryTrackingUnmeasured, charge);
    ASSERT_TRUE(waitForMemoryWorker());
    EXPECT_GE(total_memory_tracker.getRSS(), baseline_rss + charge - tolerance);
    EXPECT_LE(total_memory_tracker.getRSS(), baseline_rss + charge + tolerance);

    unmeasured.changeTo(0);
    ASSERT_TRUE(waitForMemoryWorker());
    EXPECT_GE(total_memory_tracker.getRSS(), baseline_rss - tolerance);
    EXPECT_LE(total_memory_tracker.getRSS(), baseline_rss + tolerance);

    unmeasured.changeTo(charge);
    ASSERT_TRUE(waitForMemoryWorker());
    /// Stop the worker before setting a limit below its published RSS, so incidental
    /// worker allocations cannot participate in the limit check below.
    worker.reset();

    constexpr Int64 allocation = 1LL << 20;
    const Int64 limit = baseline_rss + charge / 2;
    ASSERT_LT(total_memory_tracker.get() + allocation, limit);
    int exception_code = 0;
    bool allocated = false;
    total_memory_tracker.setHardLimit(limit);
    try
    {
        std::ignore = CurrentMemoryTracker::alloc(allocation);
        allocated = true;
    }
    catch (const DB::Exception & exception)
    {
        exception_code = exception.code();
    }
    total_memory_tracker.setHardLimit(0);
    if (allocated)
        std::ignore = CurrentMemoryTracker::free(allocation);
    EXPECT_EQ(exception_code, DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
}

}

#endif
