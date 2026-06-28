#include <Common/ThreadPoolTaskTracker.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

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

/// A scheduler that runs every callback synchronously and inline (so all task futures get a
/// proper result), except that it throws DB::Exception(CANNOT_SCHEDULE_TASK) on the Nth schedule
/// call -- modelling the thread fuzzer's CANNOT_SCHEDULE_TASK fault hitting the scheduling of the
/// final task. The callback handed to the throwing call is dropped WITHOUT running, exactly as
/// ThreadPool::scheduleImpl does when it cannot enqueue.
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

/// A scheduler that runs regular tasks inline and succeeds, but on the Nth call ACCEPTS the
/// callback (moves it in, as a real pool does on enqueue) and then drops it WITHOUT running --
/// modelling the pool enqueuing the final task and later draining it unrun during shutdown
/// (ThreadPool::worker resets the pending job via job_data.reset()). The future it returns is the
/// one the pool would hand back for the enqueue; scheduleFinalTask discards it. Dropping the
/// callback releases the captured FinalTaskState shared_ptr, so its destructor runs and must
/// satisfy the final task's already-stored future.
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

/// Regression test for the broken_promise abort (std::future_error code 1001) seen in stress
/// tests: when scheduling the final task (e.g. the async S3 completeMultipartUpload) failed with
/// CANNOT_SCHEDULE_TASK, the final packaged task was dropped without running, leaving its
/// already-stored future with a broken promise. waitAll() then threw std::future_error (a
/// std::logic_error), aborting the server in debug/sanitizer builds.
///
/// Pre-enqueue failure: the scheduler throws before the job is enqueued, so the final task never
/// runs. waitAll() must observe the real scheduling error (CANNOT_SCHEDULE_TASK) -- NOT a
/// broken-promise std::future_error, and NOT a falsely-successful future (running the callback
/// inline would mask the scheduling failure, which is what the original fix did wrong).
///
/// All priors finish on the inline scheduler before addFinal is called, so this goes through the
/// addFinal() run_final_task_now site.
TEST(TaskTrackerAddFinal, FinalTaskScheduleFailureSurfacesCannotScheduleTask)
{
    auto calls = std::make_shared<std::atomic<size_t>>(0);
    /// 3 add() calls schedule successfully, the 4th schedule (the final task) throws.
    TaskTracker tracker(throwingOnNthSchedule(calls, /*throw_on_call=*/4), /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_TRUE(tracker.isAsync());

    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < 3; ++i)
        tracker.add([&] { ran.fetch_add(1); });

    tracker.addFinal([&] { final_ran = true; });

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
    /// The final callback never ran: scheduling failed before enqueue, and the future now carries
    /// the scheduling error rather than a (masking) success.
    EXPECT_FALSE(final_ran.load());

    tracker.safeWaitAll();
}

/// Same pre-enqueue failure, but reached through the async SCOPE_EXIT call site in add() -- the
/// path that actually fires in the real S3 multipart-upload stress scenario, where regular part
/// uploads are still in flight when addFinal() registers the completeMultipartUpload task and the
/// final task is then scheduled from the last finishing part's completion handler.
///
/// A real single-thread pool is used. The one regular task blocks on a gate so it is guaranteed
/// in flight when addFinal() runs (forcing the else branch that registers final_task). Releasing
/// the gate lets the regular task finish; its SCOPE_EXIT schedules the final task (the 2nd
/// scheduler call), which throws CANNOT_SCHEDULE_TASK. waitAll() must surface that, not a broken
/// promise.
TEST(TaskTrackerAddFinal, FinalTaskScheduleFailureViaScopeExitSurfacesCannotScheduleTask)
{
    ThreadPool pool(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        /*max_threads=*/ 1);

    /// Decorate the real pool runner: forward regular schedules to the pool, but throw
    /// CANNOT_SCHEDULE_TASK on the 2nd schedule (the final task). The two schedule calls are
    /// strictly ordered -- the regular add() schedules synchronously on the test thread (call 1),
    /// and the final task is only scheduled later from the worker's SCOPE_EXIT (call 2) -- so the
    /// count deterministically targets the final task even with a real pool.
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

    std::promise<void> gate;
    std::shared_future<void> gate_future = gate.get_future().share();
    std::atomic<bool> final_ran{false};

    /// Worker blocks here until the gate is released, so the task is in flight at addFinal time.
    tracker.add([gate_future] { gate_future.wait(); });
    /// tasks_finished(0) != tasks_added(1) -> addFinal registers final_task (the SCOPE_EXIT site).
    tracker.addFinal([&] { final_ran = true; });
    /// Release -> regular task finishes -> its SCOPE_EXIT schedules the final task -> 2nd scheduler
    /// call throws CANNOT_SCHEDULE_TASK (caught in scheduleFinalTask, which sets the promise).
    gate.set_value();

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

/// Regression test for the post-enqueue / queued-drop gap raised in review: the pool accepts the
/// final task's lambda, then drops it unrun while shutting down. The scheduler future returned for
/// the enqueue is discarded, so the ONLY thing that can satisfy the final task's already-stored
/// future is FinalTaskState's destructor (the captured state is released when the dropped lambda
/// is destroyed). It must satisfy the promise with a normal CANNOT_SCHEDULE_TASK exception -- NOT
/// leave it with a broken-promise std::future_error.
///
/// A bare std::packaged_task could not cover this: destroying it unrun stores a broken-promise
/// std::future_error (a std::logic_error), which waitAll() then surfaces as a LOGICAL_ERROR and
/// aborts the server in debug/sanitizer builds. This asserts waitAll() observes CANNOT_SCHEDULE_TASK
/// on the queued-drop path, deterministically. (The destructor is call-site independent, so one
/// site -- addFinal()'s run_final_task_now branch -- is sufficient coverage for this path.)
TEST(TaskTrackerAddFinal, FinalTaskDroppedAfterEnqueueSurfacesCannotScheduleTask)
{
    auto calls = std::make_shared<std::atomic<size_t>>(0);
    /// No prior add() tasks, so addFinal takes the run_final_task_now branch: the only schedule
    /// call (call 1) is the final task, which the scheduler accepts and then drops unrun.
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
