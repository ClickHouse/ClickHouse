#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
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

        /// --- Same post-attach failure, but the borrowed thread starts UNKNOWN (initially unnamed).
        /// UNKNOWN is a valid previous name, not a "nothing to restore" sentinel, so the catch must
        /// still put it back rather than leave the thread renamed to MERGE_MUTATE. ---
        setThreadName(ThreadName::UNKNOWN); /// writes "Unknown" to the OS name; getThreadName() now reports UNKNOWN
        ASSERT_EQ(getThreadName(), ThreadName::UNKNOWN);
        FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_post_attach_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Post-attach failure must restore the original group for an initially-unnamed borrowed thread";
        }
        EXPECT_EQ(getThreadName(), ThreadName::UNKNOWN)
            << "Post-attach failure must restore an UNKNOWN previous name; the restore is gated by a bool, not by the name value";
        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

/// The destructor borrow path (allow_existing_group=true on a group-owning thread) must restore the
/// borrowed thread's NAME, not just its group. master's DroppedSynchronouslyWhileAttachedToAnotherGroup
/// covers the group restore and the no-abort, but never checks the name -- which is what this PR adds.
/// Both a real previous name and UNKNOWN (initially unnamed) must be restored: UNKNOWN is a valid
/// previous name, not a "nothing to restore" sentinel, so the restore is gated by a bool, not the value.
TEST(ThreadGroupSwitcher, RestoresBorrowedThreadName)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto G0 = std::make_shared<ThreadGroup>(context, 0);
        auto G1 = std::make_shared<ThreadGroup>(context, 0);

        for (auto prev_name : {ThreadName::TCP_HANDLER, ThreadName::UNKNOWN})
        {
            /// setThreadName(UNKNOWN) writes "Unknown" to the OS name so getThreadName() reports UNKNOWN.
            setThreadName(prev_name);
            CurrentThread::attachToGroupIfDetached(G0);
            ASSERT_EQ(getThreadName(), prev_name);

            {
                /// Borrows the group-owning thread and renames it to the async-pool name.
                ThreadGroupSwitcher switcher(G1, ThreadName::S3_COPY_POOL, /*allow_existing_group*/ true);
                EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
            } /// ~ThreadGroupSwitcher must put both the group and the name back.

            EXPECT_EQ(getCurrentThreadGroup(), G0);
            EXPECT_EQ(getThreadName(), prev_name)
                << "borrowed thread's name must be restored, not left as the async-pool name";
            CurrentThread::detachFromGroupIfNotDetached();
        }
    });
    t.join();
}

} // namespace DB
