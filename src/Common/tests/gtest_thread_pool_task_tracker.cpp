#include <Common/ThreadPoolTaskTracker.h>
#include <Common/setThreadName.h>

#include <string_view>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

using namespace DB;

namespace
{

LogSeriesLimiterPtr makeTestLogger()
{
    return std::make_shared<LogSeriesLimiter>(getLogger("TaskTrackerTest"), 1, 5);
}

struct AsyncTracker
{
    ThreadPool pool;
    TaskTracker tracker;

    explicit AsyncTracker(size_t threads, size_t max_inflight = 0)
        : pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, threads)
        , tracker(threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::UNKNOWN), max_inflight, makeTestLogger())
    {}
};

}

TEST(TaskTrackerAddFinal, AsyncHappyPathRunsFinalAfterAllPriors)
{
    AsyncTracker t{/*threads=*/4};
    constexpr size_t N = 16;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};
    std::atomic<size_t> count_observed_by_final{0};

    for (size_t i = 0; i < N; ++i)
        t.tracker.add([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            ran.fetch_add(1);
        });

    t.tracker.addFinal([&] {
        count_observed_by_final = ran.load();
        final_ran = true;
    });

    t.tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
    EXPECT_EQ(count_observed_by_final.load(), N);
}

TEST(TaskTrackerAddFinal, AsyncZeroPriorsStillRunsFinal)
{
    AsyncTracker t{/*threads=*/2};
    std::atomic<bool> final_ran{false};

    t.tracker.addFinal([&] { final_ran = true; });
    t.tracker.waitAll();

    EXPECT_TRUE(final_ran.load());
}

TEST(TaskTrackerAddFinal, AsyncAllPriorsAlreadyDoneBeforeAddFinal)
{
    AsyncTracker t{/*threads=*/2};
    constexpr size_t N = 4;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < N; ++i)
        t.tracker.add([&] { ran.fetch_add(1); });

    /// Give workers a chance to finish before we call addFinal — exercises the path where
    /// `finished_futures.size() == futures.size()` is already true at the time addFinal is called.
    while (ran.load() != N)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    t.tracker.addFinal([&] { final_ran = true; });
    t.tracker.waitAll();

    EXPECT_TRUE(final_ran.load());
}

TEST(TaskTrackerAddFinal, AsyncPriorFailureDoesNotDeadlock)
{
    AsyncTracker t{/*threads=*/2};

    t.tracker.add([] { /* ok */ });
    t.tracker.add([] { throw std::runtime_error("boom"); });
    t.tracker.add([] { /* ok */ });
    t.tracker.addFinal([] {});

    EXPECT_THROW(t.tracker.waitAll(), std::runtime_error);
    /// The final callback is best-effort on failures — we only assert no deadlock and clean shutdown.
    t.tracker.safeWaitAll();
}

TEST(TaskTrackerAddFinal, AsyncFinalCallbackException)
{
    AsyncTracker t{/*threads=*/2};
    t.tracker.add([] { /* ok */ });
    t.tracker.addFinal([] { throw std::runtime_error("final boom"); });

    EXPECT_THROW(t.tracker.waitAll(), std::runtime_error);
    t.tracker.safeWaitAll();
}

TEST(TaskTrackerAddFinal, WaitIfAnyBeforeFinalTask)
{
    TaskTracker tracker(/*scheduler_=*/{}, /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_FALSE(tracker.isAsync());

    constexpr size_t N = 2;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < N; ++i)
        tracker.add([&] { ran.fetch_add(1); });

    /// Sync runner has already run every task body and populated `finished_futures`.
    ASSERT_EQ(ran.load(), N);

    tracker.waitIfAny();

    tracker.addFinal([&] { final_ran = true; });

    tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
}

TEST(TaskTrackerAddFinal, AsyncWithInflightLimit)
{
    /// Exercise the path where `waitTilInflightShrink` is hit on the owner thread between adds.
    AsyncTracker t{/*threads=*/4, /*max_inflight=*/3};
    constexpr size_t N = 32;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < N; ++i)
        t.tracker.add([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            ran.fetch_add(1);
        });

    size_t observed_in_final = 0;
    t.tracker.addFinal([&] {
        observed_in_final = ran.load();
        final_ran = true;
    });
    t.tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
    EXPECT_EQ(observed_in_final, N);
}

TEST(TaskTrackerAddFinal, SyncRunnerHappyPath)
{
    TaskTracker tracker(/*scheduler_=*/{}, /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_FALSE(tracker.isAsync());

    constexpr size_t N = 5;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};
    size_t observed_in_final = 0;

    for (size_t i = 0; i < N; ++i)
        tracker.add([&] { ran.fetch_add(1); });

    tracker.addFinal([&] {
        observed_in_final = ran.load();
        final_ran = true;
    });
    tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
    EXPECT_EQ(observed_in_final, N);
}

TEST(TaskTrackerAddFinal, SyncRunnerPriorFailureDoesNotDeadlock)
{
    TaskTracker tracker(/*scheduler_=*/{}, /*max_tasks_inflight=*/0, makeTestLogger());

    tracker.add([] { /* ok */ });
    tracker.add([] { throw std::runtime_error("boom"); });
    tracker.addFinal([] {});

    EXPECT_THROW(tracker.waitAll(), std::runtime_error);
    tracker.safeWaitAll();
}

namespace
{

/// Run callbacks synchronously. On the Nth call, throw CANNOT_SCHEDULE_TASK.
ThreadPoolCallbackRunnerUnsafe<void> throwingOnNthSchedule(std::shared_ptr<std::atomic<size_t>> calls, size_t throw_on_call)
{
    return [calls, throw_on_call](std::function<void()> && callback, Priority) mutable -> std::future<void>
    {
        if (calls->fetch_add(1) + 1 == throw_on_call)
            throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "scheduler refused the task");

        auto package = std::packaged_task<void()>(std::move(callback));
        package();
        return package.get_future();
    };
}

/// Models the pool enqueueing the final task and later draining it unrun during shutdown.
ThreadPoolCallbackRunnerUnsafe<void> droppingOnNthSchedule(std::shared_ptr<std::atomic<size_t>> calls, size_t drop_on_call)
{
    return [calls, drop_on_call](std::function<void()> && callback, Priority) mutable -> std::future<void>
    {
        if (calls->fetch_add(1) + 1 == drop_on_call)
        {
            /// Take ownership of the callback and let it go out of scope unrun.
            [[maybe_unused]] auto dropped = std::move(callback);
            std::promise<void> discarded;
            discarded.set_value();
            return discarded.get_future();
        }

        auto package = std::packaged_task<void()>(std::move(callback));
        package();
        return package.get_future();
    };
}

}

/// Regression test for the broken_promise abort seen in stress tests.
/// Case where callback fails when run during addFinal().
TEST(TaskTrackerAddFinal, FinalTaskScheduleFailureSurfacesCannotScheduleTask)
{
    auto calls = std::make_shared<std::atomic<size_t>>(0);
    TaskTracker tracker(throwingOnNthSchedule(calls, /*throw_on_call=*/4), /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_TRUE(tracker.isAsync());

    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < 3; ++i)
        tracker.add([&] { ran.fetch_add(1); });

    tracker.addFinal([&] { final_ran = true; });

    // Verify the future carries the scheduling error.
    try
    {
        tracker.waitAll();
        FAIL() << "waitAll() did not throw -- the scheduling failure was masked as success";
    }
    catch (const std::future_error & e)
    {
        FAIL() << "final task future left with a broken promise: " << e.what();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK) << e.what();
    }

    EXPECT_EQ(ran.load(), 3u);
    EXPECT_FALSE(final_ran.load());

    tracker.safeWaitAll();
}

/// Case where callback fails when run during SCOPE_EXIT.
TEST(TaskTrackerAddFinal, FinalTaskScheduleFailureViaScopeExitSurfacesCannotScheduleTask)
{
    ThreadPool pool(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        /*max_threads=*/ 1);

    auto base = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::UNKNOWN);
    auto calls = std::make_shared<std::atomic<size_t>>(0);
    ThreadPoolCallbackRunnerUnsafe<void> scheduler =
        [base, calls](std::function<void()> && callback, Priority priority) mutable -> std::future<void>
    {
        if (calls->fetch_add(1) + 1 == 2)
            throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "scheduler refused the final task");
        return base(std::move(callback), priority);
    };

    TaskTracker tracker(scheduler, /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_TRUE(tracker.isAsync());

    // Call both add() and addFinal(). Use gate to ensure task is in flight at addFinal time.
    std::promise<void> gate;
    std::shared_future<void> gate_future = gate.get_future().share();
    std::atomic<bool> final_ran{false};
    tracker.add([gate_future] { gate_future.wait(); });
    tracker.addFinal([&] { final_ran = true; });

    /// Call set_value(). The regular task will finish and allow scheduling the final task.
    /// Scheduler throws CANNOT_SCHEDULE_TASK.
    gate.set_value();

    /// Verify waitAll() fails with CANNOT_SCHEDULE_TASK and final task doesn't run.
    try
    {
        tracker.waitAll();
        FAIL() << "waitAll() did not throw -- the scheduling failure was masked as success";
    }
    catch (const std::future_error & e)
    {
        FAIL() << "final task future left with a broken promise: " << e.what();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK) << e.what();
    }
    EXPECT_FALSE(final_ran.load());
    tracker.safeWaitAll();
}

/// Regression test for the post-enqueue gap.
TEST(TaskTrackerAddFinal, FinalTaskDroppedAfterEnqueueSurfacesCannotScheduleTask)
{
    auto calls = std::make_shared<std::atomic<size_t>>(0);
    TaskTracker tracker(droppingOnNthSchedule(calls, /*drop_on_call=*/1), /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_TRUE(tracker.isAsync());

    std::atomic<bool> final_ran{false};
    tracker.addFinal([&] { final_ran = true; });

    try
    {
        tracker.waitAll();
        FAIL() << "waitAll() did not throw -- a dropped final task was masked as success";
    }
    catch (const std::future_error & e)
    {
        FAIL() << "final task was dropped without satisfying its promise (broken promise): " << e.what();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK) << e.what();
    }

    /// The final callback never ran -- it was dropped before execution.
    EXPECT_FALSE(final_ran.load());

    tracker.safeWaitAll();
}

/// On the scheduler-throw path waitAll() must surface the scheduler's real exception -- code
/// CANNOT_SCHEDULE_TASK AND its original message -- not a masking success, not a broken-promise
/// std::future_error, and not the synthesized "dropped before execution" message the queued-drop case
/// uses. This pins the schedule_error bridge: the destructor prefers the recorded scheduler exception
/// over synthesizing its own, so the real cause reaches the waiter.
TEST(TaskTrackerAddFinal, FinalTaskScheduleFailureSurfacesSchedulerException)
{
    auto calls = std::make_shared<std::atomic<size_t>>(0);
    /// The first (and only) schedule is the final task -- the scheduler throws before enqueue.
    TaskTracker tracker(throwingOnNthSchedule(calls, /*throw_on_call=*/1), /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_TRUE(tracker.isAsync());

    tracker.addFinal([] {});

    try
    {
        tracker.waitAll();
        FAIL() << "waitAll() did not throw -- the scheduling failure was masked as success";
    }
    catch (const std::future_error & e)
    {
        FAIL() << "final task future left with a broken promise: " << e.what();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK) << e.what();
        /// The scheduler's own message must survive, proving the recorded scheduler exception is
        /// surfaced rather than a locally synthesized one.
        EXPECT_NE(std::string_view(e.what()).find("scheduler refused the task"), std::string_view::npos)
            << "scheduler-throw path lost the scheduler's exception; got: " << e.what();
    }

    tracker.safeWaitAll();
}
