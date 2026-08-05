#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/ScopedThreadAttributes.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace FailPoints
{
    extern const char attach_to_group_failure[];
    extern const char scoped_thread_attributes_post_attach_failure[];
}

/// After a failed ScopedThreadAttributes construction the thread must be left in the
/// state it was in before construction started (detached or attached to the original
/// group), so the next scoped_attributes on the same thread can attach cleanly.
TEST(ScopedThreadAttributes, FailedConstructionRestoresPreviousState)
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

        /// --- Starting detached: every failed switch must end detached, and must not keep the new
        /// name. The "keep the name after attaching from a detached thread" contract is only for a
        /// SUCCESSFUL attach (see KeepsNameAfterAttachFromDetachedThread); a failed one has no job
        /// whose tracing span would want the name. ---
        setThreadName(ThreadName::TCP_HANDLER);

        FailPointInjection::enableFailPoint(FailPoints::attach_to_group_failure);
        {
            ScopedThreadAttributes scoped_attributes(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            EXPECT_EQ(getCurrentThreadGroup(), nullptr)
                << "Failed attach from detached state must leave the thread detached";
            EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
                << "Failed attach from detached state must restore the original name for the scope body";
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);

        FailPointInjection::enableFailPoint(FailPoints::scoped_thread_attributes_post_attach_failure);
        {
            ScopedThreadAttributes scoped_attributes(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
            EXPECT_EQ(getCurrentThreadGroup(), nullptr)
                << "Post-attach failure from detached state must leave the thread detached";
            EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
                << "Post-attach failure from detached state must restore the original name for the scope body";
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);

        /// --- Starting attached to G0 and named: every failed allow_existing_group switch must
        /// restore G0 and the original name already in the catch block, not only in the destructor.
        /// The thread is renamed before the attach is attempted, and a switch that failed must not
        /// leave the temporary name installed for the scope body, or the log messages, trace spans
        /// and `thread.prof.name` produced there are attributed to a switch that never happened. ---
        setThreadName(ThreadName::TCP_HANDLER);
        CurrentThread::attachToGroupIfDetached(G0);

        FailPointInjection::enableFailPoint(FailPoints::attach_to_group_failure);
        {
            ScopedThreadAttributes scoped_attributes(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Failed allow_existing_group attach must restore the original group";
            EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
                << "Failed allow_existing_group attach must restore the original name for the scope body";
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
            << "Failed allow_existing_group attach must restore the original name";

        FailPointInjection::enableFailPoint(FailPoints::scoped_thread_attributes_post_attach_failure);
        {
            ScopedThreadAttributes scoped_attributes(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Post-attach failure must detach the target group and restore the original";
            EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
                << "Post-attach failure must restore the original name for the scope body";
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
            << "Post-attach failure must restore the original name after setThreadName renamed the thread";

        /// --- Same post-attach failure, but the borrowed thread starts UNKNOWN (initially unnamed).
        /// UNKNOWN is a valid previous name, not a "nothing to restore" sentinel, so the destructor
        /// must still put it back rather than leave the thread renamed to MERGE_MUTATE. ---
        setThreadName(ThreadName::UNKNOWN); /// writes "Unknown" to the OS name; getThreadName() now reports UNKNOWN
        ASSERT_EQ(getThreadName(), ThreadName::UNKNOWN);
        FailPointInjection::enableFailPoint(FailPoints::scoped_thread_attributes_post_attach_failure);
        {
            ScopedThreadAttributes scoped_attributes(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
            EXPECT_EQ(getCurrentThreadGroup(), G0)
                << "Post-attach failure must restore the original group for an initially-unnamed borrowed thread";
            EXPECT_EQ(getThreadName(), ThreadName::UNKNOWN)
                << "Post-attach failure must restore an UNKNOWN previous name for the scope body too";
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
TEST(ScopedThreadAttributes, RestoresBorrowedThreadName)
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
                ScopedThreadAttributes scoped_attributes(G1, ThreadName::S3_COPY_POOL, /*allow_existing_group*/ true);
                EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
            } /// ~ScopedThreadAttributes must put both the group and the name back.

            EXPECT_EQ(getCurrentThreadGroup(), G0);
            EXPECT_EQ(getThreadName(), prev_name)
                << "borrowed thread's name must be restored, not left as the async-pool name";
            CurrentThread::detachFromGroupIfNotDetached();
        }
    });
    t.join();
}

/// The name switch must not depend on group attachment: it must happen (and be undone) both
/// with no group to attach to and when already attached to the target group.
TEST(ScopedThreadAttributes, SwitchesNameIndependentlyOfGroup)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        setThreadName(ThreadName::TCP_HANDLER);

        {
            ScopedThreadAttributes scoped_attributes(nullptr, ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL)
                << "the name must be switched even with no group to attach to";
            EXPECT_EQ(getCurrentThreadGroup(), nullptr);
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);

        auto context = getContext().context;
        auto G0 = std::make_shared<ThreadGroup>(context, 0);
        CurrentThread::attachToGroupIfDetached(G0);
        {
            ScopedThreadAttributes scoped_attributes(G0, ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL)
                << "the name must be switched even when already attached to the target group";
            EXPECT_EQ(getCurrentThreadGroup(), G0);
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);
        EXPECT_EQ(getCurrentThreadGroup(), G0);
        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

/// A thread that was never renamed carries the binary name as its OS comm (e.g. "clickhouse"
/// for the server's main thread), which has no ThreadName enum value. The destructor must
/// restore that name verbatim: collapsing it through the enum would write "Unknown" as the
/// comm, permanently breaking tools that match the process by its original comm, such as
/// `pkill clickhouse` and `ps -C clickhouse` (integration tests restart the server that way).
///
/// FreeBSD and illumos cannot read the OS-level name back (getThreadNameRaw returns an empty
/// string there), so a comm absent from the enum cannot round-trip at all - those platforms use
/// the cached-ThreadName fallback covered by the next test instead.
#if !defined(OS_FREEBSD) && !defined(OS_SUNOS)
TEST(ScopedThreadAttributes, RestoresNameAbsentFromEnum)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        setThreadNameRaw("clickhouse");
        ASSERT_EQ(getThreadNameRaw(), "clickhouse");
        ASSERT_EQ(getThreadName(), ThreadName::UNKNOWN);

        {
            ScopedThreadAttributes scoped_attributes(nullptr, ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
        }

        EXPECT_EQ(getThreadNameRaw(), "clickhouse")
            << "a comm absent from the ThreadName enum must be restored verbatim, not as 'Unknown'";
    });
    t.join();
}
#endif

/// When the raw OS-level name cannot be read - FreeBSD and illumos have no raw read, and on Linux
/// `prctl(PR_GET_NAME)` can be blocked in a restricted sandbox - the snapshot carries an empty raw
/// name. The restore must then fall back to the cached ThreadName instead of doing nothing:
/// setThreadName always updates the cache (and the jemalloc profile name), which is what the logs
/// and traces report, so a no-op restore would leak the temporary name past the scope.
TEST(ScopedThreadAttributes, RestoresFromCachedNameWhenRawReadUnavailable)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        setThreadName(ThreadName::TCP_HANDLER);

        /// As returned by saveThreadName when the raw read is unavailable.
        ThreadNameSnapshot snapshot{.raw = "", .cached = ThreadName::TCP_HANDLER};

        setThreadName(ThreadName::S3_COPY_POOL);
        ASSERT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);

        restoreThreadName(snapshot);
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER)
            << "with no raw name available the restore must fall back to the cached ThreadName";
    });
    t.join();
}

/// After a successful attach from a detached thread the destructor must leave the new NAME in
/// place (while still detaching the group): ThreadPoolImpl::worker reads the name after the job
/// returns to give the tracing span a readable operation name, and resets it on the next
/// iteration. Restoring the name here would make tracing fall back to demangled lambda names.
TEST(ScopedThreadAttributes, KeepsNameAfterAttachFromDetachedThread)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto G1 = std::make_shared<ThreadGroup>(context, 0);

        setThreadName(ThreadName::DEFAULT_THREAD_POOL);
        ASSERT_EQ(getCurrentThreadGroup(), nullptr);

        {
            ScopedThreadAttributes scoped_attributes(G1, ThreadName::MERGE_MUTATE);
            EXPECT_EQ(getThreadName(), ThreadName::MERGE_MUTATE);
            EXPECT_EQ(getCurrentThreadGroup(), G1);
        }

        EXPECT_EQ(getCurrentThreadGroup(), nullptr)
            << "the destructor must detach the group attached from a detached thread";
        EXPECT_EQ(getThreadName(), ThreadName::MERGE_MUTATE)
            << "the name must be left in place after a successful attach from a detached thread, "
               "so the thread pool worker can use it as the tracing span operation name";
    });
    t.join();
}

} // namespace DB
