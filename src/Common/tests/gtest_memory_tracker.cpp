#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <future>
#include <thread>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/OvercommitTracker.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/ProcessList.h>
#include <base/scope_guard.h>

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

TEST(MemoryTracker, SpeculativeReservationRanksQueryForGlobalOvercommit)
{
    MemoryTracker first;
    MemoryTracker second;

    constexpr Int64 soft_limit = 128 * MB;
    constexpr Int64 reservation = 4 * MB;

    first.adjustWithUntrackedMemory(100 * MB);
    second.adjustWithUntrackedMemory(101 * MB);

    EXPECT_LT(first.getOvercommitRatio(soft_limit), second.getOvercommitRatio(soft_limit));

    first.addSpeculativeReservation(reservation);
    EXPECT_LT(second.getOvercommitRatio(soft_limit), first.getOvercommitRatio(soft_limit));

    first.subSpeculativeReservation(reservation);
    EXPECT_LT(first.getOvercommitRatio(soft_limit), second.getOvercommitRatio(soft_limit));
}

TEST(MemoryTracker, SpeculativeReservationDoesNotAffectUserOvercommit)
{
    MemoryTracker first;
    MemoryTracker second;

    constexpr Int64 soft_limit = 128 * MB;
    constexpr Int64 reservation = 4 * MB;

    first.setSoftLimit(soft_limit);
    second.setSoftLimit(soft_limit);
    first.adjustWithUntrackedMemory(100 * MB);
    second.adjustWithUntrackedMemory(101 * MB);

    first.addSpeculativeReservation(reservation);

    /// Speculative reservations are charged only to the total tracker, and must not
    /// change the victim selected when a user-level limit is exceeded.
    EXPECT_LT(first.getOvercommitRatio(), second.getOvercommitRatio());
}

TEST(MemoryTracker, GlobalReservationUsesOutermostProcessTracker)
{
    MemoryTracker outer_process(&total_memory_tracker, VariableContext::Process, false);
    MemoryTracker inner_process(&outer_process, VariableContext::Process, false);

    std::thread([&]
    {
        DB::ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&inner_process);
        thread_status.untracked_memory_limit = 0;

        constexpr Int64 reservation = MB;
        auto * credited_tracker = CurrentMemoryTracker::allocGlobal(reservation);
        EXPECT_EQ(credited_tracker, &outer_process);
        CurrentMemoryTracker::freeGlobal(reservation, credited_tracker);
    }).join();
}

TEST(MemoryTracker, SpeculativeReservationSurvivesExternalCorrection)
{
    /// `MemoryWorker` periodically replaces the total tracker's counters with externally
    /// measured values (`updateAllocated` from jemalloc statistics, `updateRSS` from the
    /// resident set size). A live speculative reservation is not backed by any allocation,
    /// so the measurement does not see it; the correction must add it back, otherwise the
    /// paired `freeGlobal` would push the counters below the actual memory usage and the
    /// total tracker would stop being an upper bound.
    constexpr Int64 reservation = 16 * MB;
    constexpr Int64 measured_amount = 300 * MB;
    constexpr Int64 measured_rss = 320 * MB;

    const Int64 amount_before = total_memory_tracker.get();
    const Int64 rss_before = total_memory_tracker.getRSS();
    SCOPE_EXIT({
        MemoryTracker::updateAllocated(amount_before, /*log_change=*/ false);
        MemoryTracker::updateRSS(rss_before);
    });

    /// The test thread has no thread group, so the reservation is credited to no query
    /// tracker and lands on the total tracker only.
    auto * credited_tracker = CurrentMemoryTracker::allocGlobal(reservation);
    EXPECT_EQ(credited_tracker, nullptr);
    EXPECT_LE(std::abs(total_memory_tracker.get() - (amount_before + reservation)), GLOBAL_TOLERANCE);

    /// A correction tick arrives while the reservation is live: the measured values must
    /// be raised by the reservation, not installed verbatim.
    MemoryTracker::updateAllocated(measured_amount, /*log_change=*/ false);
    MemoryTracker::updateRSS(measured_rss);
    EXPECT_LE(std::abs(total_memory_tracker.get() - (measured_amount + reservation)), GLOBAL_TOLERANCE);
    EXPECT_LE(std::abs(total_memory_tracker.getRSS() - (measured_rss + reservation)), GLOBAL_TOLERANCE);

    /// Releasing the reservation brings the counters back to exactly the measured values.
    CurrentMemoryTracker::freeGlobal(reservation, credited_tracker);
    EXPECT_LE(std::abs(total_memory_tracker.get() - measured_amount), GLOBAL_TOLERANCE);
    EXPECT_LE(std::abs(total_memory_tracker.getRSS() - measured_rss), GLOBAL_TOLERANCE);

    /// With no reservation live, a correction installs the measured values verbatim.
    MemoryTracker::updateAllocated(measured_amount, /*log_change=*/ false);
    MemoryTracker::updateRSS(measured_rss);
    EXPECT_LE(std::abs(total_memory_tracker.get() - measured_amount), GLOBAL_TOLERANCE);
    EXPECT_LE(std::abs(total_memory_tracker.getRSS() - measured_rss), GLOBAL_TOLERANCE);
}

TEST(MemoryTracker, FailedSpeculativeReservationLeavesCorrectionUntouched)
{
    /// A reservation rejected by the server-wide hard limit must not linger in the
    /// reservations counter, or every later correction would inflate the total tracker.
    constexpr Int64 measured_amount = 300 * MB;

    const Int64 amount_before = total_memory_tracker.get();
    const Int64 rss_before = total_memory_tracker.getRSS();
    const Int64 hard_limit_before = total_memory_tracker.getHardLimit();
    SCOPE_EXIT({
        total_memory_tracker.setHardLimit(hard_limit_before);
        MemoryTracker::updateAllocated(amount_before, /*log_change=*/ false);
        MemoryTracker::updateRSS(rss_before);
    });

    total_memory_tracker.setHardLimit(std::max(amount_before, rss_before) + LIMIT);
    try
    {
        std::ignore = CurrentMemoryTracker::allocGlobal(OVER_LIMIT);
        FAIL() << "Expected the total memory tracker to reject the reservation of " << OVER_LIMIT;
    }
    catch (const DB::Exception & exception)
    {
        EXPECT_EQ(exception.code(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    MemoryTracker::updateAllocated(measured_amount, /*log_change=*/ false);
    EXPECT_LE(std::abs(total_memory_tracker.get() - measured_amount), GLOBAL_TOLERANCE);
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

}
