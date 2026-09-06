#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>

#include <atomic>
#include <future>
#include <thread>
#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

using namespace DB;

/// Regression test: when `ThreadPoolCallbackRunnerFast::operator()` fails to start a worker
/// (`scheduleOrThrow` throws, e.g. `CANNOT_SCHEDULE_TASK` because the underlying pool has no
/// free slot), the just-enqueued callback and the `active_tasks` increment must be rolled back,
/// like `bulkSchedule` already does. Otherwise the caller sees a failure but the callback stays
/// queued and runs later, when another worker eventually starts - under a different query and
/// possibly after the captured state was destroyed.
TEST(ThreadPoolCallbackRunnerFast, RollbackOnScheduleFailure)
{
    /// Run in a dedicated thread so current_thread starts as nullptr, independent of whatever
    /// ThreadStatus / thread group other gtests in unit_tests_dbms left behind.
    std::thread t([]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto thread_group = std::make_shared<ThreadGroup>(context, 0);

        ThreadPool pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 2);

        ThreadPoolCallbackRunnerFast runner;
        runner.initThreadPool(pool, /*max_threads_=*/ 2, ThreadName::UNKNOWN, thread_group);

        auto failed_callback_ran = std::make_shared<std::atomic<bool>>(false);

        /// Make `pool->scheduleOrThrow` inside `startMoreThreadsIfNeeded` throw synchronously.
        CannotAllocateThreadFaultInjector::setFaultProbability(1.0);
        SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });

        bool threw_cannot_schedule = false;
        try
        {
            runner([failed_callback_ran] { failed_callback_ran->store(true); });
        }
        catch (const Exception & e)
        {
            threw_cannot_schedule = (e.code() == ErrorCodes::CANNOT_SCHEDULE_TASK);
        }

        EXPECT_TRUE(threw_cannot_schedule) << "scheduling onto a faulted pool must throw CANNOT_SCHEDULE_TASK";

        /// The failed schedule must leave no trace: `active_tasks` is decremented back...
        EXPECT_TRUE(runner.isIdle()) << "a failed schedule must roll back active_tasks";

        CannotAllocateThreadFaultInjector::setFaultProbability(0.0);

        if (runner.isIdle())
        {
            /// ...and the callback is dequeued, so a later successful schedule must not resurrect it.
            std::promise<void> second_done;
            runner([&second_done] { second_done.set_value(); });
            second_done.get_future().wait();

            runner.shutdown();

            EXPECT_FALSE(failed_callback_ran->load())
                << "a callback whose scheduling failed must never run";
        }
        else
        {
            /// Without the rollback the internal state is inconsistent (`queue` holds the leaked
            /// callback while `queue_size` was never incremented), and another schedule would
            /// strand its callback forever. The failure is already recorded above - bail out
            /// without touching the broken runner, so the test fails instead of hanging.
            runner.shutdown();
        }
    });
    t.join();
}
