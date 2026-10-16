#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
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
/// and is already attached to its own thread group. To release the callback the drop path
/// (~CallbackRunnerTask) attaches the task's group and must tolerate the thread already having one;
/// otherwise ThreadGroupSwitcher raises the `already attached to a group` LOGICAL_ERROR, which aborts
/// the server in builds that treat logical errors as assertions.
TEST(ThreadPoolCallbackRunner, DroppedSynchronouslyWhileAttachedToAnotherGroup)
{
    /// Run in a dedicated thread so current_thread starts as nullptr, independent of whatever
    /// ThreadStatus / thread group other gtests in unit_tests_dbms left behind.
    std::thread t([]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto task_group = std::make_shared<ThreadGroup>(context, 0);   /// group current when the runner is created
        auto caller_group = std::make_shared<ThreadGroup>(context, 0); /// group the scheduling thread is on

        ThreadPool pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1);

        /// Create the runner while attached to task_group (capture happens at enqueue, below -- not here).
        CurrentThread::attachToGroupIfDetached(task_group);
        auto runner = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::REMOTE_FS_WRITE_THREAD_POOL);
        CurrentThread::detachFromGroupIfNotDetached();

        /// Now attach the scheduling thread to a different group, as a running query would be.
        CurrentThread::attachToGroupIfDetached(caller_group);

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
            /// caller_group). With the bug this aborts; with the fix it unwinds cleanly.
            runner(std::move(callback), Priority{});
        }
        catch (const Exception & e)
        {
            threw_cannot_schedule = (e.code() == ErrorCodes::CANNOT_SCHEDULE_TASK);
        }

        EXPECT_TRUE(threw_cannot_schedule) << "scheduling onto a faulted pool must throw CANNOT_SCHEDULE_TASK";
        /// The runner captures the current group at ENQUEUE time, not at creation: it was created under
        /// task_group but enqueued under caller_group, so the dropped callback is released under
        /// caller_group (with the old creation-time capture it would have been task_group).
        EXPECT_EQ(*released_group, caller_group) << "dropped callback must be released under the enqueue-time group";
        /// ... and it must restore the scheduling thread's original group afterwards.
        EXPECT_EQ(getCurrentThreadGroup(), caller_group) << "the scheduling thread's group must be restored";

        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

TEST(ThreadPoolCallbackRunner, DroppedCallbacksCanProtectTheirOwnCleanup)
{
    for (bool use_local_runner : {false, true})
    {
        std::async(std::launch::async, [use_local_runner]
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(getContext().context, [] { return CancellationRequest::StopWork; });
            ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
            /// Capacity includes the blocked worker and both callbacks queued behind it.
            ThreadPool pool(
                CurrentMetrics::LocalThread,
                CurrentMetrics::LocalThreadActive,
                CurrentMetrics::LocalThreadScheduled,
                /*max_threads=*/ 1,
                /*max_free_threads=*/ 1,
                /*queue_size=*/ 3);
            std::promise<void> release;
            auto gate = release.get_future().share();
            pool.scheduleOrThrowOnError([gate]
            {
                gate.wait();
                throw std::runtime_error("drop queued callbacks");
            });
            bool released = false;
            SCOPE_EXIT_SAFE({ if (!released) release.set_value(); });
            auto unsafe = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::MERGE_MUTATE);
            ThreadPoolCallbackRunnerLocal<void> local(pool, ThreadName::MERGE_MUTATE);
            std::atomic_size_t destroyed{0};
            struct Recorder
            {
                ThreadGroupPtr group;
                std::atomic_size_t * destroyed;
                ~Recorder()
                {
                    ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE, true);
                    EXPECT_EQ(CurrentThread::get().getCancellationRequest(), CancellationRequest::StopWork);
                    ThreadStatus::QueryCancellationBlocker cleanup;
                    EXPECT_NO_THROW(CurrentThread::checkIfNotCancelled());
                    ++*destroyed;
                }
            };
            std::vector<std::future<void>> futures;
            for (size_t i = 0; i < 2; ++i)
            {
                std::function<void()> callback = [recorder = std::make_shared<Recorder>(group, &destroyed)]
                {
                    FAIL() << "Dropped callback must not execute";
                };
                if (use_local_runner)
                    futures.emplace_back(std::move(local.enqueueAndGiveOwnership(std::move(callback))->future));
                else
                    futures.emplace_back(unsafe(std::move(callback), {}));
            }
            release.set_value();
            released = true;
            EXPECT_THROW(pool.wait(), std::runtime_error);
            for (auto & future : futures)
                EXPECT_ANY_THROW(future.get());
            EXPECT_EQ(destroyed.load(), 2);
            EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
        }).get();
    }
}

TEST(ThreadPoolCallbackRunner, CallbackDestructionPrecedesExceptionalCompletion)
{
    for (bool use_local_runner : {false, true})
    {
        std::async(std::launch::async, [use_local_runner]
        {
            ThreadStatus status;
            auto group = ThreadGroup::createForMergeMutate(getContext().context);
            ThreadGroupSwitcher attached(group, ThreadName::MERGE_MUTATE);
            ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
            auto unsafe = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::MERGE_MUTATE);
            ThreadPoolCallbackRunnerLocal<void> local(pool, ThreadName::MERGE_MUTATE);
            std::atomic_bool destroyed{false};
            struct Recorder
            {
                ThreadGroupPtr group;
                std::atomic_bool * destroyed;
                ~Recorder()
                {
                    EXPECT_EQ(getCurrentThreadGroup(), group);
                    EXPECT_TRUE(ThreadStatus::QueryCancellationBlocker::isActive());
                    *destroyed = true;
                }
            };
            std::future<void> future;
            {
                ThreadStatus::QueryCancellationBlocker cleanup;
                std::function<void()> callback = [recorder = std::make_shared<Recorder>(group, &destroyed)]
                {
                    throw std::runtime_error("unwind callback");
                };
                if (use_local_runner)
                    future = std::move(local.enqueueAndGiveOwnership(std::move(callback))->future);
                else
                    future = unsafe(std::move(callback), {});
            }
            EXPECT_THROW(future.get(), std::runtime_error);
            EXPECT_TRUE(destroyed.load());
            EXPECT_FALSE(ThreadStatus::QueryCancellationBlocker::isActive());
            pool.wait();
        }).get();
    }
}
