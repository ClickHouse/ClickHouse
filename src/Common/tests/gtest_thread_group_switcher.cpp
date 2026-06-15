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
    extern const char thread_group_switcher_attach_failure[];
    extern const char thread_group_link_thread_failure[];
}

/// Mid-attachment failure must leave ThreadStatus fully detached so the next switcher
/// on the same thread can attach cleanly. Regression for incident #1682.
TEST(ThreadGroupSwitcher, PartialAttachUndoneOnException)
{
    auto context = getContext().context;

    std::exception_ptr ex;
    bool second_switcher_succeeded = false;

    std::thread t([&]
    {
        try
        {
            ThreadStatus ts;

            auto G1 = std::make_shared<ThreadGroup>(context, 0);
            auto G2 = std::make_shared<ThreadGroup>(context, 0);

            /// Enable the failpoint: attachToGroupImpl() will throw after all context
            /// fields are set (performance_counters parent, memory_tracker parent,
            /// query_context, local_data, …) but before initPerformanceCounters,
            /// matching the 26.4 failure window.
            FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_attach_failure);

            {
                /// ThreadGroupSwitcher is noexcept — the injected exception is caught
                /// and logged internally. The SCOPE_EXIT_SAFE guard in attachToGroupImpl
                /// calls detachFromGroup() to undo the full attachment before returning.
                ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);

                /// Prove the failpoint actually fired: on success the thread would
                /// be attached to G1 here. nullptr means the attach failed and the
                /// rollback in attachToGroupImpl ran.
                ASSERT_EQ(getCurrentThreadGroup(), nullptr);
            }

            /// Failpoint is ONCE — already consumed, no need to disable.

            /// With the fix the thread is clean: the second attachment must succeed.
            /// Without the fix the stale G1 is still set and this constructor throws
            /// LOGICAL_ERROR "already attached" (caught internally, switcher is a no-op).
            {
                ThreadGroupSwitcher switcher(G2, ThreadName::REMOTE_FS_READ_THREAD_POOL);
                second_switcher_succeeded = (getCurrentThreadGroup() == G2);
            }

            ASSERT_EQ(getCurrentThreadGroup(), nullptr);
        }
        catch (...)
        {
            ex = std::current_exception();
        }
    });
    t.join();

    if (ex)
        std::rethrow_exception(ex);

    EXPECT_TRUE(second_switcher_succeeded)
        << "Second ThreadGroupSwitcher must attach successfully after the first one "
           "cleaned up its partial attachment; without the fix the stale group from the "
           "failed first attachment would block every subsequent task on this pool worker";
}

/// When linkThread() itself throws (e.g. bad_alloc from thread_ids.insert), active_thread_count
/// was never incremented. The SCOPE_EXIT_SAFE rollback in attachToGroupImpl must NOT call
/// unlinkThread() in that case, or it would corrupt the counter and hit the chassert.
/// After the failure the thread must be clean and able to accept the next attachment.
TEST(ThreadGroupSwitcher, LinkThreadFailureDoesNotCorruptCounter)
{
    auto context = getContext().context;

    std::exception_ptr ex;
    bool second_switcher_succeeded = false;

    std::thread t([&]
    {
        try
        {
            ThreadStatus ts;

            auto G1 = std::make_shared<ThreadGroup>(context, 0);
            auto G2 = std::make_shared<ThreadGroup>(context, 0);

            FailPointInjection::enableFailPoint(FailPoints::thread_group_link_thread_failure);

            {
                /// linkThread throws before active_thread_count is incremented.
                /// The SCOPE_EXIT rollback must skip unlinkThread (linked=false).
                /// ThreadStatus::thread_group is never set, so the thread is immediately clean.
                ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);

                /// Prove the failpoint actually fired: on success the thread would
                /// be attached to G1 here. nullptr means linkThread threw and the
                /// thread was never attached in the first place.
                ASSERT_EQ(getCurrentThreadGroup(), nullptr);
            }

            /// Thread is clean — second attachment must succeed.
            {
                ThreadGroupSwitcher switcher(G2, ThreadName::REMOTE_FS_READ_THREAD_POOL);
                second_switcher_succeeded = (getCurrentThreadGroup() == G2);
            }

            ASSERT_EQ(getCurrentThreadGroup(), nullptr);
        }
        catch (...)
        {
            ex = std::current_exception();
        }
    });
    t.join();

    if (ex)
        std::rethrow_exception(ex);

    EXPECT_TRUE(second_switcher_succeeded)
        << "linkThread failure must not corrupt active_thread_count and must leave the "
           "thread clean for the next attachment";
}

/// allow_existing_group=true callers (ExceptionKeepingTransform, merge/mutate tasks)
/// start the switcher already attached to a group. If the new attachment fails, the
/// constructor must best-effort restore the original group so the caller does not
/// continue without query/merge accounting and cancellation context.
TEST(ThreadGroupSwitcher, AllowExistingGroupRestoresOnFailure)
{
    auto context = getContext().context;

    std::exception_ptr ex;
    bool original_group_restored = false;

    std::thread t([&]
    {
        try
        {
            ThreadStatus ts;

            auto G0 = std::make_shared<ThreadGroup>(context, 0);
            auto G1 = std::make_shared<ThreadGroup>(context, 0);

            /// Start attached to G0, simulating a merge/pipeline executor thread.
            CurrentThread::attachToGroupIfDetached(G0);
            ASSERT_EQ(getCurrentThreadGroup(), G0);

            FailPointInjection::enableFailPoint(FailPoints::thread_group_switcher_attach_failure);

            {
                /// allow_existing_group=true: constructor detaches G0, attachToGroupImpl
                /// then throws (failpoint), its rollback detaches the partial G1 state.
                /// The constructor catch block must restore G0.
                ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            }

            original_group_restored = (getCurrentThreadGroup() == G0);

            CurrentThread::detachFromGroupIfNotDetached();
        }
        catch (...)
        {
            ex = std::current_exception();
        }
    });
    t.join();

    if (ex)
        std::rethrow_exception(ex);

    EXPECT_TRUE(original_group_restored)
        << "Failed allow_existing_group switch must restore the original group; "
           "otherwise the thread loses its query/merge accounting and cancellation context";
}

} // namespace DB
