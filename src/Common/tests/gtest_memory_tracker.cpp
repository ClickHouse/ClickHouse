#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <thread>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/MemoryTrackerUntrackedAllocationsBlockerInThread.h>
#include <Common/OvercommitTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/TraceSender.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/TraceCollector.h>
#include <base/scope_guard.h>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace CurrentMetrics
{
    extern const Metric MergesMutationsMemoryTracking;
}

namespace ProfileEvents
{
    extern const Event MemoryLargeAllocationTraced;
}

namespace
{

/// Library code running in the attached thread also charges the trackers, so the tests
/// use large allocations and compare with a tolerance instead of asserting exact equality.
constexpr Int64 MB = 1024 * 1024;
constexpr Int64 TOLERANCE = MB;
constexpr Int64 GLOBAL_TOLERANCE = 64 * 1024;
constexpr Int64 LIMIT = 50 * MB;
constexpr Int64 OVER_LIMIT = 60 * MB;

struct MemoryTrackerHierarchy
{
    MemoryTracker user{&total_memory_tracker, VariableContext::User, false};
    MemoryTracker process{&user, VariableContext::Process, false};
};

struct UserOvercommitTrackerForTest : UserOvercommitTracker
{
    using UserOvercommitTracker::UserOvercommitTracker;

    void setCandidate(MemoryTracker * candidate_)
    {
        candidate = candidate_;
    }

    std::future<void> getSelectionFuture()
    {
        return query_selected.get_future();
    }

protected:
    void pickQueryToExcludeImpl() override
    {
        picked_tracker = candidate;
        query_selected.set_value();
    }

private:
    MemoryTracker * candidate = nullptr;
    std::promise<void> query_selected;
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

TEST(MemoryTracker, GlobalLimitFailureRollsBackAmountAndRSS)
{
    const Int64 amount_before = total_memory_tracker.get();
    const Int64 rss_before = total_memory_tracker.getRSS();
    const Int64 hard_limit_before = total_memory_tracker.getHardLimit();
    SCOPE_EXIT(total_memory_tracker.setHardLimit(hard_limit_before));

    total_memory_tracker.setHardLimit(std::max(amount_before, rss_before) + LIMIT);
    expectMemoryLimitExceeded(OVER_LIMIT);

    EXPECT_LE(std::abs(total_memory_tracker.get() - amount_before), GLOBAL_TOLERANCE);
    EXPECT_LE(std::abs(total_memory_tracker.getRSS() - rss_before), GLOBAL_TOLERANCE);
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

TEST(MemoryTracker, ParentLimitFailureDoesNotReleaseOvercommitWaiters)
{
    MemoryTrackerHierarchy hierarchy;
    runInThread(hierarchy, [&](MemoryTracker &)
    {
        std::ignore = CurrentMemoryTracker::alloc(OVER_LIMIT);
    });
    hierarchy.user.setHardLimit(LIMIT);

    DB::ProcessList process_list;
    DB::ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest overcommit_tracker(&process_list, &user_process_list);
    overcommit_tracker.setCandidate(&hierarchy.process);
    hierarchy.user.setOvercommitTracker(&overcommit_tracker);

    MemoryTracker waiting;
    waiting.setOvercommitWaitingTime(4'000'000);
    auto query_selected = overcommit_tracker.getSelectionFuture();
    auto wait_result = std::async(std::launch::async, [&]
    {
        return overcommit_tracker.needToStopQuery(&waiting, OVER_LIMIT);
    });

    query_selected.wait();
    runInThread(hierarchy, [&](MemoryTracker &)
    {
        expectMemoryLimitExceeded(OVER_LIMIT);
    });

    EXPECT_EQ(wait_result.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    runInThread(hierarchy, [&](MemoryTracker &)
    {
        std::ignore = CurrentMemoryTracker::free(OVER_LIMIT);
    });
    EXPECT_EQ(wait_result.get(), OvercommitResult::MEMORY_FREED);
    expectUsage(hierarchy, 0);
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

TEST(MemoryTracker, FaultInjectionFailureRollsBackHierarchy)
{
    MemoryTrackerHierarchy hierarchy;
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


/// min_allocation_size_to_log_stack_trace: a stack trace for one large allocation charged to the
/// global tracker. These cases charge the tracker directly, so no real memory is ever allocated and
/// the sizes can be large enough that one unmatched add dwarfs the accounting noise.

constexpr Int64 TRACE_THRESHOLD = 64 * MB;
constexpr Int64 QUALIFYING_ALLOCATION = 128 * MB;
/// Mirrors max_large_allocations_traced in MemoryTracker.cpp.
constexpr ProfileEvents::Count TRACE_BUDGET = 10;

ProfileEvents::Count tracedLargeAllocations()
{
    return ProfileEvents::global_counters[ProfileEvents::MemoryLargeAllocationTraced];
}

/// Charges the *global* tracker with exactly `size`, which the threshold cases depend on. A
/// ThreadStatus (this thread has one or not depending on the other tests in the binary) batches
/// charges and flushes a whole batch as one add, so no remainder may be pending around a charge.
void chargeAndRelease(Int64 size, size_t times = 1)
{
    for (size_t i = 0; i < times; ++i)
    {
        DB::CurrentThread::flushUntrackedMemory();
        std::ignore = CurrentMemoryTracker::alloc(size);
        std::ignore = CurrentMemoryTracker::free(size);
    }

    DB::CurrentThread::flushUntrackedMemory();
}

/// A live collector is required twice over: the setter refuses a threshold without one, and its
/// thread is what runs the symbolize-and-log path, which the destructor drains before returning.
/// Constructing one in SetUp also refills the trace budget, so every case below starts from a full
/// one and can assert exact counts.
class MemoryTrackerLargeAllocationTrace : public ::testing::Test
{
protected:
    void SetUp() override
    {
        collector = std::make_unique<DB::TraceCollector>();
        MemoryTracker::setMinAllocationSizeToLogStackTrace(TRACE_THRESHOLD);
        ASSERT_EQ(MemoryTracker::getMinAllocationSizeToLogStackTrace(), static_cast<UInt64>(TRACE_THRESHOLD));
    }

    void TearDown() override
    {
        MemoryTracker::setMinAllocationSizeToLogStackTrace(0);
        collector.reset();
    }

private:
    std::unique_ptr<DB::TraceCollector> collector;
};

TEST_F(MemoryTrackerLargeAllocationTrace, FiresOnTheUnblockedPath)
{
    const auto before = tracedLargeAllocations();
    chargeAndRelease(QUALIFYING_ALLOCATION);

    EXPECT_EQ(tracedLargeAllocations(), before + 1);
}

TEST_F(MemoryTrackerLargeAllocationTrace, FiresOnTheBlockedGlobalPath)
{
    const auto before = tracedLargeAllocations();
    {
        /// This path updates the global amount and returns before commitAllocation, so it needs its
        /// own detection: unmodified master reports nothing at all for an allocation charged here.
        MemoryTrackerBlockerInThread blocker(VariableContext::Global);
        chargeAndRelease(QUALIFYING_ALLOCATION);
    }

    EXPECT_EQ(tracedLargeAllocations(), before + 1);
}

TEST_F(MemoryTrackerLargeAllocationTrace, SilentBelowTheThreshold)
{
    const auto before = tracedLargeAllocations();
    chargeAndRelease(TRACE_THRESHOLD - 1);

    EXPECT_EQ(tracedLargeAllocations(), before);
}

TEST_F(MemoryTrackerLargeAllocationTrace, FiresAtExactlyTheThreshold)
{
    const auto before = tracedLargeAllocations();
    chargeAndRelease(TRACE_THRESHOLD);

    /// The setting is documented as a minimum, so the comparison is inclusive: this is the case
    /// that distinguishes it from a strict one.
    EXPECT_EQ(tracedLargeAllocations(), before + 1);
}

TEST_F(MemoryTrackerLargeAllocationTrace, FiresAtExactlyTheThresholdOnTheBlockedGlobalPath)
{
    const auto before = tracedLargeAllocations();
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::Global);
        chargeAndRelease(TRACE_THRESHOLD);
    }

    /// The second detect site carries its own copy of the comparison, so it needs its own case.
    EXPECT_EQ(tracedLargeAllocations(), before + 1);
}

TEST_F(MemoryTrackerLargeAllocationTrace, TracedUnderTheUntrackedAllocationsBlocker)
{
    const auto before = tracedLargeAllocations();
    {
        /// Pins that this diagnostic is deliberately not gated on this blocker, unlike the sibling
        /// MemoryAllocatedWithoutCheck telemetry: it is held across system-log enqueues and whole
        /// ZooKeeper thread loops, which are candidate origins for the allocation being hunted.
        MemoryTrackerUntrackedAllocationsBlockerInThread blocker;
        chargeAndRelease(QUALIFYING_ALLOCATION);
    }

    EXPECT_EQ(tracedLargeAllocations(), before + 1);
}

TEST_F(MemoryTrackerLargeAllocationTrace, StopsFiringOnceTheBudgetIsSpent)
{
    const size_t batch = TRACE_BUDGET + 10;

    const auto before = tracedLargeAllocations();
    chargeAndRelease(QUALIFYING_ALLOCATION, batch);
    const auto first = tracedLargeAllocations() - before;

    chargeAndRelease(QUALIFYING_ALLOCATION, batch);
    const auto second = tracedLargeAllocations() - before - first;

    /// A batch larger than the budget spends exactly the budget, and the next one is silent: without
    /// the bound each batch would trace `batch` times.
    EXPECT_EQ(first, TRACE_BUDGET);
    EXPECT_EQ(second, 0u);
}

TEST_F(MemoryTrackerLargeAllocationTrace, BudgetIsNotRaisedByReconfiguration)
{
    chargeAndRelease(QUALIFYING_ALLOCATION, TRACE_BUDGET + 1);
    const auto spent = tracedLargeAllocations();

    /// Re-applying the value is what a configuration reload does, and switching it off and on again
    /// is the obvious way to ask for more. Neither raises the bound: only a new collector does.
    MemoryTracker::setMinAllocationSizeToLogStackTrace(TRACE_THRESHOLD);
    chargeAndRelease(QUALIFYING_ALLOCATION);
    EXPECT_EQ(tracedLargeAllocations(), spent);

    MemoryTracker::setMinAllocationSizeToLogStackTrace(0);
    MemoryTracker::setMinAllocationSizeToLogStackTrace(TRACE_THRESHOLD);
    chargeAndRelease(QUALIFYING_ALLOCATION);
    EXPECT_EQ(tracedLargeAllocations(), spent);
}

TEST_F(MemoryTrackerLargeAllocationTrace, DoesNotDisturbAccounting)
{
    const Int64 before = total_memory_tracker.get();
    chargeAndRelease(QUALIFYING_ALLOCATION, TRACE_BUDGET + 10);

    /// The collector thread symbolizes under a Global blocker, and those allocations are charged
    /// here too, so this is a tolerance rather than an equality. A single leaked or double-counted
    /// add would be QUALIFYING_ALLOCATION, well above it.
    EXPECT_LT(std::abs(total_memory_tracker.get() - before), QUALIFYING_ALLOCATION / 2);
}

TEST(MemoryTrackerLargeAllocationTraceDefaults, IsInertByDefault)
{
    ASSERT_EQ(MemoryTracker::getMinAllocationSizeToLogStackTrace(), 0u);

    const auto before = tracedLargeAllocations();
    chargeAndRelease(QUALIFYING_ALLOCATION);

    EXPECT_EQ(tracedLargeAllocations(), before);
}

TEST(MemoryTrackerLargeAllocationTraceDefaults, RefusedWithoutTraceCollector)
{
    ASSERT_FALSE(DB::TraceSender::isCollecting());

    MemoryTracker::setMinAllocationSizeToLogStackTrace(TRACE_THRESHOLD);
    SCOPE_EXIT(MemoryTracker::setMinAllocationSizeToLogStackTrace(0));

    /// Without a collector the trace could only be captured and discarded, so the setter stores 0
    /// and system.server_settings reports that rather than the configured value.
    EXPECT_EQ(MemoryTracker::getMinAllocationSizeToLogStackTrace(), 0u);

    const auto before = tracedLargeAllocations();
    chargeAndRelease(QUALIFYING_ALLOCATION);

    EXPECT_EQ(tracedLargeAllocations(), before);
}

}
