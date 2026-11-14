#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/FiberTaskTracker.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

#include <silk/fibers/future.h>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <thread>

namespace ProfileEvents
{
    extern const Event FileOpen;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

namespace FailPoints
{
    extern const char silk_fiber_task_tracker_spawn_failure[];
    extern const char silk_fiber_main_failure[];
}

}

namespace
{

class SilkFiberTaskTrackerTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};

}

TEST_F(SilkFiberTaskTrackerTest, TaskRunsInQueryGroupAndReleasesCallbackBeforeWaiterResumes)
{
    /// A dedicated thread with its own `ThreadStatus` and thread group, which waits for the tasks.
    std::thread thread([]
    {
        DB::ThreadStatus thread_status;
        auto thread_group = std::make_shared<DB::ThreadGroup>(getContext().context, 0);
        DB::ThreadGroupSwitcher switcher(thread_group, DB::ThreadName::MERGE_MUTATE);

        auto captured = std::make_shared<int>(0);
        std::weak_ptr<int> captured_weak = captured;
        DB::ThreadGroupPtr group_in_task;
        bool bound_to_os_thread = true;

        {
            Silk::FiberTaskTracker tracker(/* max_tasks= */ 1, /* max_inflight= */ 1, DB::getCurrentThreadGroup());
            const auto use_count_before_task = thread_group.use_count();
            tracker.add([&, owned = std::move(captured)]
            {
                group_in_task = DB::getCurrentThreadGroup();
                bound_to_os_thread = DB::current_thread->boundToOSThread();
                ProfileEvents::increment(ProfileEvents::FileOpen);
            });
            tracker.waitAll();

            /// The callback and the slot's group reference are released before `waitAll` returns.
            EXPECT_TRUE(captured_weak.expired());
            EXPECT_EQ(thread_group.use_count(), use_count_before_task + 1); /// `group_in_task`.
        }

        EXPECT_EQ(group_in_task, thread_group);
        EXPECT_FALSE(bound_to_os_thread);
        EXPECT_EQ(thread_group->performance_counters[ProfileEvents::FileOpen], 1);
        /// The waiting thread did not lose its own group.
        EXPECT_EQ(DB::getCurrentThreadGroup(), thread_group);
    });
    thread.join();
}

TEST_F(SilkFiberTaskTrackerTest, WaitAllRethrowsFirstErrorAndSafeWaitAllWaitsForTheRest)
{
    silk::FiberFuture gate;
    std::atomic<bool> slow_task_finished = false;

    Silk::FiberTaskTracker tracker(/* max_tasks= */ 2, /* max_inflight= */ 0, nullptr);
    tracker.add([] { throw std::runtime_error("first"); });
    tracker.add([&]
    {
        gate.wait();
        slow_task_finished = true;
    });

    EXPECT_THROW(
        {
            try
            {
                tracker.waitAll();
            }
            catch (const std::runtime_error & e)
            {
                EXPECT_STREQ(e.what(), "first");
                throw;
            }
        },
        std::runtime_error);
    EXPECT_FALSE(slow_task_finished);

    gate.set(0);
    tracker.safeWaitAll();
    EXPECT_TRUE(slow_task_finished);
}

TEST_F(SilkFiberTaskTrackerTest, InflightLimitHoldsAndFailedTaskStopsAdmission)
{
    std::atomic<size_t> active = 0;
    std::atomic<size_t> max_active = 0;
    std::atomic<size_t> finished = 0;
    std::atomic<bool> task_after_failure_ran = false;
    bool add_after_failure_threw = false;

    /// The waiter is a fiber, so `add` waits through `waitForMultiple` in a fiber.
    EXPECT_EQ(Silk::runBlocking([&]
    {
        {
            Silk::FiberTaskTracker tracker(/* max_tasks= */ 10, /* max_inflight= */ 2, nullptr);
            for (size_t i = 0; i < 10; ++i)
            {
                tracker.add([&]
                {
                    size_t now = ++active;
                    size_t prev = max_active.load();
                    while (prev < now && !max_active.compare_exchange_weak(prev, now))
                    {
                    }
                    silk::FiberScheduler::yield();
                    --active;
                    ++finished;
                });
            }
            tracker.waitAll();
        }

        {
            Silk::FiberTaskTracker tracker(/* max_tasks= */ 2, /* max_inflight= */ 1, nullptr);
            tracker.add([] { throw std::runtime_error("failed part"); });
            try
            {
                tracker.add([&] { task_after_failure_ran = true; });
            }
            catch (const std::runtime_error &)
            {
                add_after_failure_threw = true;
            }
        }
        return 0;
    }), 0);

    EXPECT_EQ(finished, 10);
    EXPECT_LE(max_active, 2);
    EXPECT_TRUE(add_after_failure_threw);
    EXPECT_FALSE(task_after_failure_ran);
}

TEST_F(SilkFiberTaskTrackerTest, TaskThatFailedToStartIsNotTrackedAndStartedTasksAreWaitedFor)
{
    silk::FiberFuture gate;
    std::atomic<bool> started_task_finished = false;
    auto captured = std::make_shared<int>(0);
    std::weak_ptr<int> captured_weak = captured;
    int error_code = 0;

    Silk::FiberTaskTracker tracker(/* max_tasks= */ 2, /* max_inflight= */ 0, nullptr);
    tracker.add([&]
    {
        gate.wait();
        started_task_finished = true;
    });

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::silk_fiber_task_tracker_spawn_failure);
    try
    {
        tracker.add([owned = std::move(captured)] {});
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }
    EXPECT_EQ(error_code, DB::ErrorCodes::CANNOT_SCHEDULE_TASK);
    EXPECT_TRUE(captured_weak.expired());
    EXPECT_FALSE(started_task_finished);

    gate.set(0);
    tracker.waitAll();
    EXPECT_TRUE(started_task_finished);
}

TEST_F(SilkFiberTaskTrackerTest, FiberThatFinishedBeforeRunningTaskIsReportedAndCallbackReleased)
{
    std::atomic<bool> task_ran = false;
    auto captured = std::make_shared<int>(0);
    std::weak_ptr<int> captured_weak = captured;
    int error_code = 0;

    Silk::FiberTaskTracker tracker(/* max_tasks= */ 1, /* max_inflight= */ 1, nullptr);
    /// The fiber returns `EIO` without an exception of the task.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::silk_fiber_main_failure);
    tracker.add([&task_ran, owned = std::move(captured)] { task_ran = true; });
    try
    {
        tracker.waitAll();
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }

    EXPECT_EQ(error_code, DB::ErrorCodes::CANNOT_SCHEDULE_TASK);
    EXPECT_FALSE(task_ran);
    EXPECT_TRUE(captured_weak.expired());
}

#endif
