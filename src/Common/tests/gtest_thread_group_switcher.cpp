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

        /// --- Starting attached to G0: every failed switch must restore G0. ---
        CurrentThread::attachToGroupIfDetached(G0);

        FailPointInjection::enableFailPoint(FailPoints::attach_to_group_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Failed allow_existing_group attach must restore the original group";
        }

        FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_post_attach_failure);
        {
            ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Post-attach failure must detach the target group and restore the original";
        }
        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

} // namespace DB
