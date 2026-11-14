#include <thread>

#include <gtest/gtest.h>

#if defined(OS_LINUX)
#include <sys/prctl.h>
#endif

#include <base/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/PerCPUMemory.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event MemoryAllocatedWithoutCheck;
    extern const Event MemoryAllocatedWithoutCheckBytes;
}

namespace DB
{

namespace FailPoints
{
    extern const char attach_to_group_failure[];
    extern const char thread_group_switcher_post_attach_failure[];
}

/// After a failed ThreadGroupSwitcher construction the thread must be left in the
/// state it was in before construction started (detached or attached to the original
/// group, and with its original name), so the next switcher on the same thread can attach cleanly.
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
        /// Both must leave the thread in its pre-construction state. The name is renamed before
        /// the group part, so it must be put back right away, not only in the destructor: the scope
        /// body must not run under the name of a switch that did not happen.
        for (const char * fail_point : {FailPoints::attach_to_group_failure, FailPoints::thread_group_switcher_post_attach_failure})
        {
            /// --- Starting detached: must end detached. ---
            setThreadName(ThreadName::TCP_HANDLER);
            FailPointInjection::enableFailPoint(fail_point);
            {
                ThreadGroupSwitcher switcher(G1, ThreadName::REMOTE_FS_READ_THREAD_POOL);
                EXPECT_EQ(getCurrentThreadGroup(), nullptr) << fail_point;
                EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER) << fail_point;
            }
            EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER) << fail_point;

            /// --- Starting attached to G0 (borrowed with allow_existing_group): must restore G0 and the
            /// name, including UNKNOWN, which is a valid previous name and not a "nothing to restore" sentinel. ---
            for (auto prev_name : {ThreadName::TCP_HANDLER, ThreadName::UNKNOWN})
            {
                setThreadName(prev_name); /// setThreadName(UNKNOWN) writes "Unknown" to the OS name
                CurrentThread::attachToGroupIfDetached(G0);
                FailPointInjection::enableFailPoint(fail_point);
                {
                    ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE, /*allow_existing_group*/ true);
                    EXPECT_EQ(getCurrentThreadGroup(), G0) << fail_point;
                    EXPECT_EQ(getThreadName(), prev_name) << fail_point;
                }
                EXPECT_EQ(getCurrentThreadGroup(), G0) << fail_point;
                EXPECT_EQ(getThreadName(), prev_name) << fail_point;
                CurrentThread::detachFromGroupIfNotDetached();
            }
        }
    });
    t.join();
}

/// Borrowing a group-owning thread (allow_existing_group=true) must restore its name together with
/// the group, else it keeps the async-pool name and misattributes later logs, traces and profiles.
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
            setThreadName(prev_name);
            CurrentThread::attachToGroupIfDetached(G0);
            {
                ThreadGroupSwitcher switcher(G1, ThreadName::S3_COPY_POOL, /*allow_existing_group*/ true);
                EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
                EXPECT_EQ(getCurrentThreadGroup(), G1);
            }
            EXPECT_EQ(getCurrentThreadGroup(), G0);
            EXPECT_EQ(getThreadName(), prev_name);
            CurrentThread::detachFromGroupIfNotDetached();
        }
    });
    t.join();
}

/// The name is switched (and restored) even when the group part does nothing: no group to attach
/// to, or already attached to the target group.
TEST(ThreadGroupSwitcher, SwitchesNameIndependentlyOfGroup)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto G0 = std::make_shared<ThreadGroup>(getContext().context, 0);
        setThreadName(ThreadName::TCP_HANDLER);

        {
            ThreadGroupSwitcher switcher(nullptr, ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getCurrentThreadGroup(), nullptr);
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);

        CurrentThread::attachToGroupIfDetached(G0);
        {
            ThreadGroupSwitcher switcher(G0, ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getCurrentThreadGroup(), G0);
        }
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);
        EXPECT_EQ(getCurrentThreadGroup(), G0);
        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

/// After a successful attach from a detached thread (a pool worker running a job) the name stays:
/// ThreadPoolImpl::worker reads it after the job to name the tracing span, and renames the thread
/// back itself. Restoring it here would make tracing fall back to demangled lambda names.
TEST(ThreadGroupSwitcher, KeepsNameAfterAttachFromDetachedThread)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto G1 = std::make_shared<ThreadGroup>(getContext().context, 0);
        setThreadName(ThreadName::DEFAULT_THREAD_POOL);

        {
            ThreadGroupSwitcher switcher(G1, ThreadName::MERGE_MUTATE);
            EXPECT_EQ(getThreadName(), ThreadName::MERGE_MUTATE);
            EXPECT_EQ(getCurrentThreadGroup(), G1);
        }
        EXPECT_EQ(getCurrentThreadGroup(), nullptr);
        EXPECT_EQ(getThreadName(), ThreadName::MERGE_MUTATE);
    });
    t.join();
}

#if defined(OS_LINUX)
/// A thread that was never renamed carries the binary name as its OS comm ("clickhouse" for the
/// server's main thread), which has no ThreadName value. It must be restored verbatim: restoring
/// through the enum would write "Unknown", after which `pkill clickhouse` and `ps -C clickhouse`
/// (how integration tests restart the server) no longer match.
TEST(ThreadGroupSwitcher, RestoresNameAbsentFromEnum)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        /// Not pthread_setname_np/pthread_getname_np: glibc-compatibility stubs them out.
        ASSERT_EQ(prctl(PR_SET_NAME, "clickhouse", 0, 0, 0), 0);
        ASSERT_EQ(getThreadName(), ThreadName::UNKNOWN);

        {
            ThreadGroupSwitcher switcher(nullptr, ThreadName::S3_COPY_POOL);
            EXPECT_EQ(getThreadName(), ThreadName::S3_COPY_POOL);
        }

        char comm[16] = {};
        ASSERT_EQ(prctl(PR_GET_NAME, comm, 0, 0, 0), 0);
        EXPECT_STREQ(comm, "clickhouse");
        EXPECT_EQ(getThreadName(), ThreadName::UNKNOWN);
    });
    t.join();
}
#endif

/// Where the OS comm cannot be read (FreeBSD, illumos, WASM, a sandbox blocking `prctl(PR_GET_NAME)`)
/// the snapshot falls back to the cached ThreadName, which is what the logs and traces report, so the
/// restore must still move it back.
TEST(ThreadGroupSwitcher, RestoresFromCachedNameWhenCommUnavailable)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        setThreadName(ThreadName::S3_COPY_POOL);
        restoreThreadName(ThreadNamePair{.comm = "", .cached = ThreadName::TCP_HANDLER});
        EXPECT_EQ(getThreadName(), ThreadName::TCP_HANDLER);
    });
    t.join();
}

/// Reparenting a thread's `MemoryTracker` flushes the untracked balance the thread carried in, and that
/// flush increments `MemoryAllocatedWithoutCheck` and `MemoryAllocatedWithoutCheckBytes` on whatever
/// `ProfileEvents::Counters` chain is in force. Those bytes predate the group, so they must not appear on
/// the group's counters: `system.query_log` reports the group's counters as the query's.
TEST(ThreadGroupAttach, PreAttachUntrackedMemoryIsNotChargedToTheGroup)
{
    /// A dedicated thread so `current_thread` starts as nullptr and the balance below is this thread's
    /// alone, independent of whatever other gtests in unit_tests_dbms left behind.
    std::thread t([&]
    {
        ThreadStatus ts;
        auto group = std::make_shared<ThreadGroup>(getContext().context, 0);

        /// Any allocation between the seeding below and `setParent` can commit the balance through
        /// the shared per-CPU budget, whose occupancy other threads control. Lifting the budget
        /// leaves the per-thread cap asserted below as the only trigger that can commit it.
        const Int64 saved_budget = per_cpu_memory.budgetCapacity();
        per_cpu_memory.setBudgetCapacity(PerCPUMemory::UNLIMITED_BUDGET);
        SCOPE_EXIT({ per_cpu_memory.setBudgetCapacity(saved_budget); });

        const auto group_events_before = group->performance_counters[ProfileEvents::MemoryAllocatedWithoutCheck];
        const auto group_bytes_before = group->performance_counters[ProfileEvents::MemoryAllocatedWithoutCheckBytes];
        const auto global_bytes_before = ProfileEvents::global_counters[ProfileEvents::MemoryAllocatedWithoutCheckBytes];

        /// Written straight into the counter instead of allocated: a real allocation of this size can
        /// be committed early by the shared per-CPU budget, which no setting of ours controls.
        constexpr Int64 pre_attach_bytes = 1024 * 1024;
        ts.untracked_memory.add(pre_attach_bytes);

        /// `~ThreadStatus` flushes whatever balance is left into `total_memory_tracker`, so the seed must
        /// be netted out on the early return of a failed assertion below too. One unconditional subtraction
        /// covers both paths (the balance is a running total), and it belongs after the tracker is reparented.
        SCOPE_EXIT({
            CurrentThread::detachFromGroupIfNotDetached();
            ts.untracked_memory.add(-pre_attach_bytes);
            ts.flushUntrackedMemory();
        });

        /// The balance is still pending: any commit would have zeroed it.
        ASSERT_GE(ts.untracked_memory.load(), pre_attach_bytes);
        ASSERT_LT(ts.untracked_memory.load(), ts.untracked_memory_limit);

        CurrentThread::attachToGroup(group);

        EXPECT_EQ(group->performance_counters[ProfileEvents::MemoryAllocatedWithoutCheckBytes], group_bytes_before)
            << "untracked memory carried into the group must not be reported as the group's";
        EXPECT_EQ(group->performance_counters[ProfileEvents::MemoryAllocatedWithoutCheck], group_events_before)
            << "the flush event itself must not be reported as the group's either";
        /// Attribution, not suppression: the flush is still counted, outside the group.
        EXPECT_GE(
            ProfileEvents::global_counters[ProfileEvents::MemoryAllocatedWithoutCheckBytes] - global_bytes_before,
            static_cast<UInt64>(pre_attach_bytes));
    });
    t.join();
}

} // namespace DB
