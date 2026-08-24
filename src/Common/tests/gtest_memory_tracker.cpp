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
