#include <functional>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace FailPoints
{
    extern const char attach_to_group_failure[];
    extern const char thread_group_switcher_post_attach_failure[];
}

/// After a failed ThreadGroupSwitcher construction the thread must be left in the
/// state it was in before construction started (detached or attached to the original
/// group), so the next switcher on the same thread can attach cleanly.
TEST(ThreadGroupSwitcher, FailedConstructionRestoresPreviousState)
{
    /// Run in a dedicated thread so current_thread starts as nullptr, independent of
    /// whatever ThreadStatus / thread group other gtests in unit_tests_dbms left behind.
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto G0 = std::make_shared<ThreadGroup>(context, 0);
        auto G1 = std::make_shared<ThreadGroup>(context, 0);

        /// attach_to_group_failure throws inside attachToGroupImpl (the attach is rolled
        /// back there); post_attach_failure throws after attachToGroup already succeeded.
        /// Both must leave the thread in its pre-construction state.

        /// --- Starting detached: every failed switch must end detached. ---
        FailPointInjection::enableFailPoint(FailPoints::attach_to_group_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            EXPECT_EQ(getCurrentThreadGroup(), nullptr)
                << "Failed attach from detached state must leave the thread detached";
        }

        FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_post_attach_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            EXPECT_EQ(getCurrentThreadGroup(), nullptr)
                << "Post-attach failure from detached state must leave the thread detached";
        }

        /// --- Starting attached to G0 and named: every failed allow_existing_group switch must
        /// restore both G0 and the original name (post_attach_failure throws after setThreadName
        /// has already renamed the thread, so the catch block must put the name back too). ---
        setThreadName(ThreadName::TCP_HANDLER);
        CurrentThread::attachToGroupIfDetached(G0);

        FailPointInjection::enableFailPoint(FailPoints::attach_to_group_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Failed allow_existing_group attach must restore the original group";
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
            << "Failed allow_existing_group attach must restore the original name";

        FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_post_attach_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Post-attach failure must detach the target group and restore the original";
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
            << "Post-attach failure must restore the original name after setThreadName renamed the thread";
        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

/// A CallbackRunnerTask whose pool throws from scheduleOrThrowOnError() (e.g. shutting down) is
/// destroyed during unwinding ON THE SCHEDULING THREAD -- which usually already owns its own query
/// thread group. ~CallbackRunnerTask() runs a ThreadGroupSwitcher to release the callback under the
/// task's group; it must NOT abort with "Thread is already attached to a group", and must leave the
/// scheduling thread attached to its original group AND wearing its original name afterwards.
/// Reproduces the arm_tsan stress abort (STID 4298-42a4): DROP TABLE ->
/// MergeTreeDeduplicationLog::shutdown -> WriteBufferFromS3 -> TaskTracker::add ->
/// scheduleOrThrowOnError throws -> ~CallbackRunnerTask on the query thread.
TEST(ThreadGroupSwitcher, CallbackRunnerTaskDestroyedOnGroupOwningThread)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto query_group = std::make_shared<ThreadGroup>(context, 0); /// the scheduling thread's own group
        auto task_group = std::make_shared<ThreadGroup>(context, 0);  /// the group the task would run under

        /// The scheduling thread is attached to its query group and named like the real query thread
        /// (e.g. a TCPHandler running DROP TABLE), so we can check the name is restored, not just the group.
        setThreadName(ThreadName::TCP_HANDLER);
        CurrentThread::attachToGroupIfDetached(query_group);
        ASSERT_EQ(getThreadName(), ThreadName::TCP_HANDLER);

        std::future<void> future;
        {
            /// Build the task that threadPoolCallbackRunnerUnsafe would have scheduled, then let it be
            /// destroyed here (never run) -- the same shared_ptr destruction that happens when
            /// scheduleOrThrowOnError() throws and unwinding drops the not-yet-queued task. Use a real
            /// (non-UNKNOWN) async-pool name so ~CallbackRunnerTask actually renames this thread; with
            /// ThreadName::UNKNOWN the rename is a no-op and the name-restore path is not exercised.
            detail::CallbackRunnerTask<void, std::function<void()>> task(
                task_group, ThreadName::S3_COPY_POOL, std::function<void()>([]{}));
            future = task.promise.get_future();
        } /// ~CallbackRunnerTask() runs here, on a thread that already owns query_group.

        /// Must not have aborted, and the scheduling thread must still own its original group AND name --
        /// the switcher renamed it to the async-pool name while borrowing and must put both back.
        EXPECT_EQ(getCurrentThreadGroup(), query_group)
            << "~CallbackRunnerTask on a group-owning thread must restore that thread's group";
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
            << "~CallbackRunnerTask must restore the borrowed thread's original name, not leave it as the async-pool name";

        /// The dropped task satisfies its promise with a normal, catchable exception.
        EXPECT_THROW(future.get(), DB::Exception);

        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

} // namespace DB
