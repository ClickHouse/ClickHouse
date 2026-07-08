#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>

#include <functional>
#include <future>
#include <vector>
#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

using namespace DB;

/// Regression test for the broken-promise abort (STID 2508-38c6).
///
/// A task scheduled through threadPoolCallbackRunnerUnsafe that the pool drops unrun during
/// shutdown must NOT leave its std::promise unsatisfied. A std::packaged_task destroyed unrun
/// stores a broken-promise std::future_error (a std::logic_error) into the shared state; when a
/// waiter observes it via future.get(), the server reports it as a LOGICAL_ERROR
/// (getCurrentExceptionMessageAndPattern -> abortOnFailedAssertion), aborting the process.
///
/// The runner must instead store a normal DB::Exception so future.get() throws an ordinary,
/// catchable error and nothing aborts. DB::Exception is NOT a std::logic_error, so even if the
/// waiter feeds it through the server's exception reporting it will not abort.
TEST(ThreadPoolCallbackRunner, DroppedOnShutdownDoesNotBreakPromise)
{
    for (size_t iteration = 0; iteration < 20; ++iteration)
    {
        constexpr size_t num_victims = 32;

        std::vector<std::future<void>> victim_futures;
        victim_futures.reserve(num_victims);

        std::promise<void> release;
        std::shared_future<void> release_future = release.get_future().share();

        {
            /// shutdown_on_exception = true (default): when the blocker throws, the worker sets
            /// shutdown=true and then drains the remaining queued jobs without running them.
            ThreadPool pool(
                CurrentMetrics::LocalThread,
                CurrentMetrics::LocalThreadActive,
                CurrentMetrics::LocalThreadScheduled,
                /*max_threads=*/ 1,
                /*max_free_threads=*/ 1,
                /*queue_size=*/ 1000);

            /// Occupy the single worker. It blocks until we release it, guaranteeing the victim
            /// tasks below are all queued behind it. On release it throws, which triggers shutdown.
            pool.scheduleOrThrowOnError([release_future]()
            {
                release_future.wait();
                throw std::runtime_error("trigger shutdown_on_exception");
            });

            auto runner = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::UNKNOWN);
            for (size_t i = 0; i < num_victims; ++i)
                victim_futures.push_back(runner([]() {}, Priority{}));

            /// Release the blocker -> it throws -> worker sets shutdown and drains the victims unrun.
            release.set_value();

            try
            {
                pool.wait();
            }
            catch (const std::exception & e)
            {
                /// The blocker's std::runtime_error is rethrown here; expected. Catch the concrete
                /// std::exception (not catch-all) so anything unexpected propagates and fails the test.
                SUCCEED() << "pool.wait() rethrew the expected shutdown exception: " << e.what();
            }
            /// Pool destructor (finalize) runs at scope exit and joins the worker.
        }

        /// Each victim either ran (set_value) or was dropped on shutdown. A dropped task must carry
        /// a normal DB::Exception, never a broken-promise std::future_error. Catch future_error
        /// specifically and fail if we see it -- that is the bug.
        for (auto & future : victim_futures)
        {
            ASSERT_TRUE(future.valid());
            try
            {
                future.get();
            }
            catch (const std::future_error & e)
            {
                FAIL() << "iteration " << iteration
                       << ": victim task was dropped without satisfying its promise (broken promise): "
                       << e.what();
            }
            catch (const Exception & e)
            {
                /// Expected for dropped tasks: a normal, catchable DB::Exception (CANNOT_SCHEDULE_TASK).
                EXPECT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK) << e.what();
            }
        }
    }
}

/// Regression test for the "already attached to a group" abort (STID 4298-3e9f).
///
/// The task above is dropped by the pool worker while draining the queue, i.e. on a thread with no
/// thread group attached. But a task can also be dropped SYNCHRONOUSLY on the scheduling thread:
/// when scheduleOrThrowOnError() throws (the pool is shutting down, or -- as injected here -- a
/// thread-allocation fault), the just-created job lambda that solely owns the CallbackRunnerTask is
/// destroyed during stack unwinding on the caller's thread. That thread is typically running a query
/// and is already attached to its own thread group. The drop path (~CallbackRunnerTask) must release
/// the callback under the task's group WITHOUT aborting: constructing a LOGICAL_ERROR would abort the
/// server in builds that treat logical errors as assertions.
///
/// The runner captures the current thread group at ENQUEUE time (getCurrentThreadGroupForAsyncCallback,
/// see PR #108988), not when the long-lived runner object was created -- a borrowed group captured at
/// creation could outlive its parent query and be attached on a pool thread (a use-after-free). So the
/// task's group equals the scheduling thread's own group, and the synchronous drop releases the callback
/// under that group. This test attaches the scheduling thread to a group DIFFERENT from the one that was
/// current when the runner was created, and verifies the drop releases under the enqueue-time group
/// (not the creation-time one) and does not abort.
TEST(ThreadPoolCallbackRunner, DroppedSynchronouslyWhileAttachedToAnotherGroup)
{
    /// Run in a dedicated thread so current_thread starts as nullptr, independent of whatever
    /// ThreadStatus / thread group other gtests in unit_tests_dbms left behind.
    std::thread t([]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto creation_group = std::make_shared<ThreadGroup>(context, 0); /// current when the runner is created
        auto enqueue_group = std::make_shared<ThreadGroup>(context, 0);  /// current when the callback is enqueued (and dropped)

        ThreadPool pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1);

        /// Create the runner while attached to creation_group. This group is intentionally NOT the one
        /// that should be captured: the runner captures the group at enqueue time, not here.
        CurrentThread::attachToGroupIfDetached(creation_group);
        auto runner = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::REMOTE_FS_WRITE_THREAD_POOL);
        CurrentThread::detachFromGroupIfNotDetached();

        /// Now attach the scheduling thread to a different group, as a running query would be. This is
        /// the group the runner must capture at enqueue time and release the dropped callback under.
        CurrentThread::attachToGroupIfDetached(enqueue_group);

        /// First, prove the enqueue-time capture is actually applied on the RUN path, where it is
        /// observable. Schedule a task that succeeds: it runs on the pool worker thread (which starts
        /// detached), and the callback records the group active inside its body. Correct enqueue-time
        /// capture attaches the worker to enqueue_group for the duration of the callback; a regression
        /// that captured nullptr (e.g. reverting to creation-time/borrowed-group handling) would leave
        /// the worker detached and record nullptr. The synchronous-drop assertions below cannot catch
        /// that regression on their own -- the drop runs on the scheduling thread, already attached to
        /// enqueue_group, so ThreadGroupSwitcher(nullptr) is a no-op and the recorder still sees
        /// enqueue_group. This run-path check is what guards the non-borrowed enqueue-time contract.
        auto worker_group = std::make_shared<ThreadGroupPtr>();
        runner([worker_group]() { *worker_group = getCurrentThreadGroup(); }, Priority{}).get();
        EXPECT_EQ(*worker_group, enqueue_group)
            << "the worker callback must run under the enqueue-time thread group (a detached worker "
               "reporting nullptr means enqueue-time capture regressed)";

        /// Make scheduleOrThrowOnError() throw synchronously on this thread.
        CannotAllocateThreadFaultInjector::setFaultProbability(1.0);
        SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });

        /// Records the thread group active when the dropped callback's captures are released.
        auto released_group = std::make_shared<ThreadGroupPtr>();
        struct GroupRecorder
        {
            std::shared_ptr<ThreadGroupPtr> out;
            explicit GroupRecorder(std::shared_ptr<ThreadGroupPtr> out_) : out(std::move(out_)) {}
            ~GroupRecorder() { *out = getCurrentThreadGroup(); }
        };
        std::function<void()> callback = [rec = std::make_shared<GroupRecorder>(released_group)]() {};

        bool threw_cannot_schedule = false;
        try
        {
            /// Throws while unwinding, dropping the sole task owner on this thread (attached to
            /// enqueue_group). With the abort bug this aborts; otherwise it unwinds cleanly.
            runner(std::move(callback), Priority{});
        }
        catch (const Exception & e)
        {
            threw_cannot_schedule = (e.code() == ErrorCodes::CANNOT_SCHEDULE_TASK);
        }

        EXPECT_TRUE(threw_cannot_schedule) << "scheduling onto a faulted pool must throw CANNOT_SCHEDULE_TASK";
        /// The runner captures the group at enqueue time, so the dropped callback is released under the
        /// enqueue-time group -- which happens to equal the scheduling thread's own group, so the drop-path
        /// ThreadGroupSwitcher is a no-op. The creation-time group must NOT be used.
        EXPECT_EQ(*released_group, enqueue_group) << "dropped callback must be released under the enqueue-time thread group";
        EXPECT_NE(*released_group, creation_group) << "the group captured when the runner was created must not be used";
        /// ... and the scheduling thread's own group is unchanged.
        EXPECT_EQ(getCurrentThreadGroup(), enqueue_group) << "the scheduling thread's group must be preserved";

        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}
