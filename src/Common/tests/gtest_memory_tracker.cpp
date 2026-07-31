#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <future>
#include <thread>
#include <tuple>
#include <vector>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace CurrentMetrics
{
    extern const Metric MergesMutationsMemoryTracking;
}

namespace
{

/// The tests drive allocations only through the public `CurrentMemoryTracker` interface
/// (no friend access to `MemoryTracker` internals), so that this file also compiles
/// against sources without the fix and reproduces the accounting leak at runtime there.
/// This is what the "Bugfix validation (unit tests)" CI job requires of a regression test.
///
/// The hierarchy is rooted at `total_memory_tracker`: in debug and sanitizer builds
/// `setParent` requires every tracker chain to terminate there. The throwing ancestor in
/// the tests is the user-level tracker, which rejects the allocation before recursing to
/// the total tracker, so the total tracker is never charged by a failing allocation.
///
/// Library code running in the attached thread (`ThreadStatus` bookkeeping, exception
/// construction) also charges the trackers with a few KB of incidental allocations, so
/// the tests allocate tens of megabytes and compare with a 1 MiB tolerance instead of
/// asserting exact equality.
constexpr Int64 MB = 1024 * 1024;
constexpr Int64 TOLERANCE = MB;
constexpr Int64 LIMIT = 50 * MB;
constexpr Int64 OVER_LIMIT = 60 * MB;

struct MemoryTrackerHierarchy
{
    MemoryTracker user{&total_memory_tracker, VariableContext::User, false};
    MemoryTracker process{&user, VariableContext::Process, false};
};

void expectNear(Int64 value, Int64 expected)
{
    EXPECT_LE(std::abs(value - expected), TOLERANCE);
}

void expectUsage(const MemoryTrackerHierarchy & hierarchy, Int64 expected)
{
    expectNear(hierarchy.process.get(), expected);
    expectNear(hierarchy.user.get(), expected);
}

void expectPeaks(const MemoryTrackerHierarchy & hierarchy, Int64 expected)
{
    expectNear(hierarchy.process.getPeak(), expected);
    expectNear(hierarchy.user.getPeak(), expected);
}

/// Run `body` in a fresh thread whose thread-level memory tracker is attached to the
/// custom hierarchy, with untracked-memory batching disabled so that every
/// `CurrentMemoryTracker` call reaches the trackers immediately.
void runInThread(MemoryTrackerHierarchy & hierarchy, const std::function<void(MemoryTracker &)> & body)
{
    std::thread([&]
    {
        DB::ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&hierarchy.process);
        thread_status.untracked_memory_limit = 0;

        body(thread_status.memory_tracker);

        /// Detach from the test hierarchy before `ThreadStatus` is destroyed, so that
        /// destruction-time bookkeeping cannot touch trackers that die with the test.
        thread_status.memory_tracker.setParent(&total_memory_tracker);
    }).join();
}

void expectMemoryLimitExceeded(Int64 size)
{
    try
    {
        std::ignore = CurrentMemoryTracker::alloc(size);
        FAIL() << "Expected the memory tracker to reject the allocation of " << size;
    }
    catch (const DB::Exception & exception)
    {
        EXPECT_EQ(exception.code(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
}

TEST(MemoryTracker, ParentLimitFailureRollsBackHierarchy)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker & thread_tracker)
    {
        const Int64 thread_before = thread_tracker.get();
        expectMemoryLimitExceeded(OVER_LIMIT);
        expectNear(thread_tracker.get(), thread_before);
    });

    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, ParentLimitFailureDoesNotUpdatePeaks)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        expectMemoryLimitExceeded(OVER_LIMIT);
    });

    expectUsage(hierarchy, 0);
    expectPeaks(hierarchy, 0);
}

TEST(MemoryTracker, ParentLimitFailureDoesNotUpdateMetrics)
{
    MemoryTrackerHierarchy hierarchy;
    const auto metric = CurrentMetrics::MergesMutationsMemoryTracking;
    const auto metric_before = CurrentMetrics::get(metric);
    hierarchy.process.setMetric(metric);
    hierarchy.user.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        expectMemoryLimitExceeded(OVER_LIMIT);
        expectNear(CurrentMetrics::get(metric), metric_before);

        /// Prove the metric is actually wired up: a successful allocation must move it.
        std::ignore = CurrentMemoryTracker::alloc(32 * MB);
        expectNear(CurrentMetrics::get(metric), metric_before + 32 * MB);

        std::ignore = CurrentMemoryTracker::free(32 * MB);
        expectNear(CurrentMetrics::get(metric), metric_before);
    });
}

TEST(MemoryTracker, RepeatedParentLimitFailuresDoNotAccumulate)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        for (size_t attempt = 0; attempt < 100; ++attempt)
        {
            expectMemoryLimitExceeded(OVER_LIMIT);
            expectUsage(hierarchy, 0);
        }
    });
}

TEST(MemoryTracker, ConcurrentParentLimitFailuresDoNotAccumulate)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(LIMIT);

    std::vector<std::future<void>> attempts;
    for (size_t attempt = 0; attempt < 16; ++attempt)
    {
        attempts.emplace_back(std::async(std::launch::async, [&hierarchy]
        {
            runInThread(hierarchy, [&](MemoryTracker &)
            {
                expectMemoryLimitExceeded(OVER_LIMIT);
            });
        }));
    }

    for (auto & attempt : attempts)
        attempt.get();

    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, ParentLimitFailurePreservesExistingUsage)
{
    MemoryTrackerHierarchy hierarchy;

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        std::ignore = CurrentMemoryTracker::alloc(40 * MB);
        expectUsage(hierarchy, 40 * MB);

        hierarchy.user.setHardLimit(LIMIT);
        expectMemoryLimitExceeded(OVER_LIMIT);
        expectUsage(hierarchy, 40 * MB);

        std::ignore = CurrentMemoryTracker::free(40 * MB);
        expectUsage(hierarchy, 0);
    });
}

TEST(MemoryTracker, ProcessLimitFailureRollsBackDescendants)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.process.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker & thread_tracker)
    {
        const Int64 thread_before = thread_tracker.get();
        expectMemoryLimitExceeded(OVER_LIMIT);
        expectNear(thread_tracker.get(), thread_before);
    });

    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, SuccessfulAllocationChargesAndFreesHierarchy)
{
    MemoryTrackerHierarchy hierarchy;

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        std::ignore = CurrentMemoryTracker::alloc(64 * MB);
        expectUsage(hierarchy, 64 * MB);

        std::ignore = CurrentMemoryTracker::free(64 * MB);
        expectUsage(hierarchy, 0);
    });
}

TEST(MemoryTracker, FaultInjectionFailureRollsBackHierarchy)
{
    MemoryTrackerHierarchy hierarchy;
    /// The public setter caps the probability at 0.5, so the fault is not guaranteed on
    /// the first attempt; the chance that 64 attempts all miss is 2^-64.
    hierarchy.user.setFaultProbability(1.0);

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        bool fault_triggered = false;
        for (size_t attempt = 0; attempt < 64 && !fault_triggered; ++attempt)
        {
            try
            {
                std::ignore = CurrentMemoryTracker::alloc(OVER_LIMIT);
                std::ignore = CurrentMemoryTracker::free(OVER_LIMIT);
            }
            catch (const DB::Exception & exception)
            {
                EXPECT_EQ(exception.code(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
                fault_triggered = true;
            }
        }
        EXPECT_TRUE(fault_triggered);
    });

    expectUsage(hierarchy, 0);
}

TEST(MemoryTracker, IgnoredLimitFailureKeepsAllocation)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        {
            /// In no-throw scopes (e.g. destructors) the limit must be ignored and the
            /// allocation must be accounted, not rolled back.
            LockMemoryExceptionInThread lock(VariableContext::Global);
            EXPECT_NO_THROW(std::ignore = CurrentMemoryTracker::alloc(OVER_LIMIT));
        }

        expectUsage(hierarchy, OVER_LIMIT);

        std::ignore = CurrentMemoryTracker::free(OVER_LIMIT);
        expectUsage(hierarchy, 0);
    });
}

TEST(MemoryTracker, LimitEnforcementCanBeDisabled)
{
    MemoryTrackerHierarchy hierarchy;
    hierarchy.user.setHardLimit(LIMIT);

    runInThread(hierarchy, [&](MemoryTracker &)
    {
        EXPECT_NO_THROW(std::ignore = CurrentMemoryTracker::allocNoThrow(OVER_LIMIT));
        expectUsage(hierarchy, OVER_LIMIT);

        std::ignore = CurrentMemoryTracker::free(OVER_LIMIT);
        expectUsage(hierarchy, 0);
    });
}

}
