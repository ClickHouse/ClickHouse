#include <gtest/gtest.h>

#include <future>
#include <vector>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace CurrentMetrics
{
    extern const Metric MergesMutationsMemoryTracking;
}

struct MemoryTrackerTestAccess
{
    static AllocationTrace alloc(MemoryTracker & tracker, Int64 size, bool enforce_memory_limit = true)
    {
        return tracker.allocImpl(size, enforce_memory_limit);
    }

    static AllocationTrace free(MemoryTracker & tracker, Int64 size)
    {
        return tracker.free(size);
    }

    static void setMetric(MemoryTracker & tracker, CurrentMetrics::Metric metric)
    {
        tracker.setMetric(metric);
    }

    static void setProfilerLimit(MemoryTracker & tracker, Int64 value)
    {
        tracker.profiler_limit.store(value, std::memory_order_relaxed);
    }

    static void setFaultProbability(MemoryTracker & tracker, double value)
    {
        /// The public `setFaultProbability` caps the probability at 0.5, but the tests
        /// need a deterministic fault, so write the raw value directly.
        tracker.fault_probability.store(value, std::memory_order_relaxed);
    }

    static Int64 getProfilerLimit(const MemoryTracker & tracker)
    {
        return tracker.profiler_limit.load(std::memory_order_relaxed);
    }

    static Int64 rollback(MemoryTracker & tracker, Int64 size)
    {
        return tracker.rollbackAllocation(size);
    }
};

namespace
{

struct MemoryTrackerHierarchy
{
    MemoryTracker global{nullptr, VariableContext::Global, false};
    MemoryTracker user{&global, VariableContext::User, false};
    MemoryTracker process{&user, VariableContext::Process, false};
    MemoryTracker thread{&process, VariableContext::Thread, false};
};

void expectUsage(const MemoryTrackerHierarchy & hierarchy, Int64 expected)
{
    EXPECT_EQ(hierarchy.thread.get(), expected);
    EXPECT_EQ(hierarchy.process.get(), expected);
    EXPECT_EQ(hierarchy.user.get(), expected);
    EXPECT_EQ(hierarchy.global.get(), expected);
}

void expectPeaks(const MemoryTrackerHierarchy & hierarchy, Int64 expected)
{
    EXPECT_EQ(hierarchy.thread.getPeak(), expected);
    EXPECT_EQ(hierarchy.process.getPeak(), expected);
    EXPECT_EQ(hierarchy.user.getPeak(), expected);
    EXPECT_EQ(hierarchy.global.getPeak(), expected);
}

TEST(MemoryTracker, ParentLimitFailureRollsBackHierarchy)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.global.setHardLimit(100);

    try
    {
        std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101);
        FAIL() << "Expected the global memory limit to reject the allocation";
    }
    catch (const DB::Exception & exception)
    {
        EXPECT_EQ(exception.code(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    expectUsage(hierarchy, 0);
    EXPECT_EQ(hierarchy.global.getRSS(), 0);
}

TEST(MemoryTracker, ParentLimitFailureDoesNotUpdatePeaks)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.global.setHardLimit(100);

    EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101), DB::Exception);
    expectUsage(hierarchy, 0);
    expectPeaks(hierarchy, 0);
}

TEST(MemoryTracker, ParentLimitFailureDoesNotAdvanceProfilerLimit)
{
    MemoryTrackerHierarchy hierarchy;
    MemoryTrackerTestAccess::setProfilerLimit(hierarchy.user, 50);
    hierarchy.global.setHardLimit(100);

    EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101), DB::Exception);
    EXPECT_EQ(MemoryTrackerTestAccess::getProfilerLimit(hierarchy.user), 50);
}

TEST(MemoryTracker, ParentLimitFailureDoesNotUpdateMetrics)
{
    MemoryTrackerHierarchy hierarchy;
    const auto metric = CurrentMetrics::MergesMutationsMemoryTracking;
    const auto metric_before = CurrentMetrics::get(metric);
    MemoryTrackerTestAccess::setMetric(hierarchy.user, metric);
    hierarchy.global.setHardLimit(100);

    EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101), DB::Exception);
    EXPECT_EQ(CurrentMetrics::get(metric), metric_before);

    /// Prove the metric is actually wired up: a successful allocation must move it.
    std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 32);
    EXPECT_EQ(CurrentMetrics::get(metric), metric_before + 32);

    std::ignore = MemoryTrackerTestAccess::free(hierarchy.thread, 32);
    EXPECT_EQ(CurrentMetrics::get(metric), metric_before);
}

TEST(MemoryTracker, RepeatedParentLimitFailuresDoNotAccumulate)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.global.setHardLimit(100);

    for (size_t attempt = 0; attempt < 100; ++attempt)
    {
        EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101), DB::Exception);
        expectUsage(hierarchy, 0);
    }
}

TEST(MemoryTracker, ConcurrentParentLimitFailuresDoNotAccumulate)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.global.setHardLimit(100);

    std::vector<std::future<void>> attempts;
    for (size_t attempt = 0; attempt < 16; ++attempt)
    {
        attempts.emplace_back(std::async(std::launch::async, [&hierarchy]
        {
            EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101), DB::Exception);
        }));
    }

    for (auto & attempt : attempts)
        attempt.get();

    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, ParentLimitFailurePreservesExistingUsage)
{
    MemoryTrackerHierarchy hierarchy;

    std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 40);
    expectUsage(hierarchy, 40);
    EXPECT_EQ(hierarchy.global.getRSS(), 40);

    hierarchy.global.setHardLimit(100);
    EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 61), DB::Exception);
    expectUsage(hierarchy, 40);
    EXPECT_EQ(hierarchy.global.getRSS(), 40);

    std::ignore = MemoryTrackerTestAccess::free(hierarchy.thread, 40);
    expectUsage(hierarchy, 0);
    EXPECT_EQ(hierarchy.global.getRSS(), 0);
}

TEST(MemoryTracker, UserLimitFailureRollsBackDescendants)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(100);

    EXPECT_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101), DB::Exception);
    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, SuccessfulAllocationChargesAndFreesHierarchy)
{
    MemoryTrackerHierarchy hierarchy;

    std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 64);
    expectUsage(hierarchy, 64);

    std::ignore = MemoryTrackerTestAccess::free(hierarchy.thread, 64);
    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, FaultInjectionFailureRollsBackHierarchy)
{
    MemoryTrackerHierarchy hierarchy;
    MemoryTrackerTestAccess::setFaultProbability(hierarchy.user, 1.0);

    try
    {
        std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 64);
        FAIL() << "Expected the injected fault to reject the allocation";
    }
    catch (const DB::Exception & exception)
    {
        EXPECT_EQ(exception.code(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    expectUsage(hierarchy, 0);
    EXPECT_EQ(hierarchy.global.getRSS(), 0);
}

TEST(MemoryTracker, IgnoredLimitFailureKeepsAllocation)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.global.setHardLimit(100);

    {
        /// In no-throw scopes (e.g. destructors) the limit must be ignored and the
        /// allocation must be accounted, not rolled back.
        LockMemoryExceptionInThread lock(VariableContext::Global);
        EXPECT_NO_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101));
    }

    expectUsage(hierarchy, 101);

    std::ignore = MemoryTrackerTestAccess::free(hierarchy.thread, 101);
    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, RollbackSaturatesAtZero)
{
    MemoryTracker global{nullptr, VariableContext::Global, false};
    MemoryTracker user{&global, VariableContext::User, false};

    std::ignore = MemoryTrackerTestAccess::alloc(user, 40);
    EXPECT_EQ(user.get(), 40);

    EXPECT_EQ(MemoryTrackerTestAccess::rollback(user, 100), 40);
    EXPECT_EQ(user.get(), 0);

    EXPECT_EQ(MemoryTrackerTestAccess::rollback(global, 40), 40);
    EXPECT_EQ(global.get(), 0);
}

TEST(MemoryTracker, LimitEnforcementCanBeDisabled)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.global.setHardLimit(100);

    EXPECT_NO_THROW(std::ignore = MemoryTrackerTestAccess::alloc(hierarchy.thread, 101, false));
    expectUsage(hierarchy, 101);

    std::ignore = MemoryTrackerTestAccess::free(hierarchy.thread, 101);
    expectUsage(hierarchy, 0);
}

}
