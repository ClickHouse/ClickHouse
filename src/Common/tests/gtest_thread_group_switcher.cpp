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
    extern const char thread_group_switcher_attach_failure[];
}

/// After a failed ThreadGroupSwitcher construction the thread must be left in the
/// state it was in before construction started (detached or attached to the original
/// group), so the next switcher on the same thread can attach cleanly.
TEST(ThreadGroupSwitcher, FailedConstructionRestoresPreviousState)
{
    ThreadStatus ts;
    auto context = getContext().context;
    auto G0 = std::make_shared<ThreadGroup>(context, 0);
    auto G1 = std::make_shared<ThreadGroup>(context, 0);

    /// Thread was detached → must end detached after the failed switch.
    FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_attach_failure);
    {
        ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        EXPECT_EQ(getCurrentThreadGroup(), nullptr)
            << "Failed switch from detached state must leave the thread detached";
    }

    /// Thread was attached to G0 → must end attached to G0 after the failed switch.
    CurrentThread::attachToGroupIfDetached(G0);
    FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_attach_failure);
    {
        ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
        EXPECT_EQ(getCurrentThreadGroup(), G0)
            << "Failed allow_existing_group switch must restore the original group";
    }
    CurrentThread::detachFromGroupIfNotDetached();
}

} // namespace DB
