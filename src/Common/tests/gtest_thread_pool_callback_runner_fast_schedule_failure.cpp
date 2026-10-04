#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

using namespace DB;

/// Regression test for a stuck task after a failed `ThreadPoolCallbackRunnerFast::operator()`.
///
/// `operator()` pushes the callback into `queue` and increments `active_tasks` before
/// `startMoreThreadsIfNeeded` tries to occupy a slot in the underlying `ThreadPool`. If
/// `ThreadPool::scheduleOrThrow` throws, `operator()` propagates the exception, so the caller
/// rightfully believes the callback was never accepted. Without rolling the enqueue back, the
/// callback stayed in `queue` while the corresponding `queue_size` ticket was never posted, which
/// has two visible consequences:
///  * the abandoned callback runs later anyway, on the ticket of an unrelated task, even though its
///    scheduler was told the scheduling failed;
///  * that unrelated task is starved of its own ticket, so it never runs at all - the stuck task.
///
/// `CannotAllocateThreadFaultInjector::setFaultProbability(1.0)` makes `scheduleOrThrow` throw
/// synchronously, which makes this path deterministic.
TEST(ThreadPoolCallbackRunnerFast, FailedScheduleDoesNotStealTheNextTasksTicket)
{
    ThreadPool pool(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        /*max_threads=*/ 1);

    ThreadPoolCallbackRunnerFast runner;
    /// One worker thread, so the failed schedule and the following one compete for the same slot.
    runner.initThreadPool(pool, /*max_threads=*/ 1, ThreadName::UNKNOWN, /*thread_group_=*/ nullptr);
    SCOPE_EXIT({ runner.shutdown(); });

    std::atomic<bool> abandoned_ran {false};
    std::promise<void> accepted_ran;
    std::future<void> accepted_future = accepted_ran.get_future();

    {
        CannotAllocateThreadFaultInjector::setFaultProbability(1.0);
        SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });

        /// No thread can be started, so the enqueue must be rolled back and the callback dropped.
        EXPECT_THROW(runner([&] { abandoned_ran.store(true); }), Exception);
    }

    /// Nothing is in flight: the failed schedule must have given `active_tasks` back.
    EXPECT_TRUE(runner.isIdle()) << "a failed schedule must not leave an active task behind";

    /// This one is accepted and must actually run. With the bug it never does, because the worker
    /// thread consumes its single ticket running the abandoned callback above.
    runner([&] { accepted_ran.set_value(); });

    ASSERT_EQ(accepted_future.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "the accepted callback got stuck: its ticket was consumed by the abandoned one";
    accepted_future.get();

    EXPECT_FALSE(abandoned_ran.load()) << "a callback whose scheduling threw must not run";
}
