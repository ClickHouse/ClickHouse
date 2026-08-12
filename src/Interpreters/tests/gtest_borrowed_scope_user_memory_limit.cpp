#include <future>
#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace ProfileEvents
{
    extern const Event SelectedRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

/// Regression coverage for user-level memory limits on borrowed-scope async work.
///
/// Async work scheduled from a borrowed `ThreadGroup` (materialized views, async-insert flushes,
/// `EXPLAIN ANALYZE`) may run after the query has already left the process list. `ProcessList`
/// wires `max_memory_usage_for_user` onto `user_memory_tracker`, which used to be reset - limits
/// included - as soon as the user's last query left the process list, so exactly that late async
/// work escaped the user limit. The test is written against observable behaviour and symbols that
/// exist without the fix, so the `Bugfix validation (unit tests)` job can build it on the merge
/// base and reproduce the bug there.
///
/// See also `src/Common/tests/gtest_borrowed_thread_group_lifetime.cpp`.

TEST(BorrowedThreadGroupLifetime, AsyncWorkAfterLastQueryOfUserObeysUserMemoryLimit)
{
    std::thread t([&]
    {
        ThreadStatus ts;

        auto context = Context::createCopy(getContext().context);
        context->makeQueryContext();

        ClientInfo client_info = context->getClientInfo();
        client_info.current_user = "borrowed_scope_test_user";
        context->setClientInfo(client_info);
        context->setCurrentQueryId("borrowed_scope_user_memory_limit_test_query");

        /// Small enough for the async allocation below to exceed it.
        context->setSetting("max_memory_usage_for_user", UInt64(16 << 20));
        /// Disable memory overcommit so exceeding the hard limit throws right away.
        context->setSetting("memory_overcommit_ratio_denominator_for_user", UInt64(0));
        context->setSetting("memory_usage_overcommit_max_wait_microseconds", UInt64(0));

        auto pool = std::make_unique<ThreadPool>(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1,
            /*max_free_threads=*/ 1,
            /*queue_size=*/ 10);

        ProcessList process_list;

        auto root = std::make_shared<ThreadGroup>(context, 0);
        CurrentThread::attachToGroupIfDetached(root);

        /// Registers the query group under `user_memory_tracker` and applies
        /// `max_memory_usage_for_user` to it. `is_internal = true` sidesteps the workload
        /// scheduler machinery, which is irrelevant here and not set up in the test context.
        auto entry = process_list.insert(
            "SELECT 1", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context,
            /*watch_start_nanoseconds*/ 0, /*is_internal*/ true);

        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);
        CurrentThread::detachFromGroupIfNotDetached();

        /// The async work is gated so that it allocates only after the query has left the
        /// process list - the exact window where the user limit used to be reset away.
        std::promise<void> query_finished;
        std::shared_future<void> query_finished_future = query_finished.get_future().share();

        CurrentThread::attachToGroupIfDetached(borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<bool>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto allocation_threw = runner([query_finished_future]
        {
            query_finished_future.wait();

            /// Large enough to exceed both the user limit and any per-thread
            /// untracked-memory batching.
            constexpr Int64 allocation = 64 << 20;
            try
            {
                std::ignore = CurrentMemoryTracker::alloc(allocation);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    throw;
                return true;
            }
            std::ignore = CurrentMemoryTracker::free(allocation);
            return false;
        }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        /// The last query of the user leaves the process list.
        entry.reset();
        query_finished.set_value();

        EXPECT_TRUE(allocation_threw.get())
            << "async allocation from a borrowed scope must still obey `max_memory_usage_for_user` "
               "after the user's last query left the process list";
        pool->wait();
    });
    t.join();
}

/// A flush of the async-insert queue runs under its own process-list entry, which may belong to a
/// different user than the caller that triggered the flush (`SYSTEM FLUSH ASYNC INSERT QUEUE` by
/// user A flushing inserts of user B). From `ProcessList::insert` on, the flush scope charges user
/// B's memory tracker - so late async work of the flush must (1) keep obeying user B's limits after
/// B's last query left the process list, and (2) NOT keep user A's trackers lingering: A's next
/// query must be able to lower `max_memory_usage_for_user` while the flush's async work is still
/// running.
TEST(BorrowedThreadGroupLifetime, FlushForAnotherUserDoesNotPinFlushingUsersLimits)
{
    std::thread t([&]
    {
        ThreadStatus ts;

        /// User A: triggers the flush; generous user limit.
        auto context_a = Context::createCopy(getContext().context);
        context_a->makeQueryContext();
        ClientInfo client_info_a = context_a->getClientInfo();
        client_info_a.current_user = "borrowed_scope_flushing_user";
        context_a->setClientInfo(client_info_a);
        context_a->setCurrentQueryId("borrowed_scope_flushing_user_query");
        context_a->setSetting("max_memory_usage_for_user", UInt64(1 << 30));
        context_a->setSetting("memory_overcommit_ratio_denominator_for_user", UInt64(0));
        context_a->setSetting("memory_usage_overcommit_max_wait_microseconds", UInt64(0));

        /// User B: owns the flushed inserts; small enough limit for the async allocation to exceed.
        auto context_b = Context::createCopy(getContext().context);
        context_b->makeQueryContext();
        ClientInfo client_info_b = context_b->getClientInfo();
        client_info_b.current_user = "borrowed_scope_inserting_user";
        context_b->setClientInfo(client_info_b);
        context_b->setCurrentQueryId("borrowed_scope_inserting_user_query");
        context_b->setSetting("max_memory_usage_for_user", UInt64(16 << 20));
        context_b->setSetting("memory_overcommit_ratio_denominator_for_user", UInt64(0));
        context_b->setSetting("memory_usage_overcommit_max_wait_microseconds", UInt64(0));

        auto pool = std::make_unique<ThreadPool>(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1,
            /*max_free_threads=*/ 1,
            /*queue_size=*/ 10);

        ProcessList process_list;

        /// User A's query, the outer scope the flush group is created from.
        auto root_a = std::make_shared<ThreadGroup>(context_a, 0);
        CurrentThread::attachToGroupIfDetached(root_a);
        auto entry_a = process_list.insert(
            "SYSTEM FLUSH ASYNC INSERT QUEUE", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context_a,
            /*watch_start_nanoseconds*/ 0, /*is_internal*/ true);

        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context_b, root_a);
        CurrentThread::detachFromGroupIfNotDetached();

        /// The flush registers its own process-list entry for user B - this re-points the flush
        /// group's memory tracker from user A's accounting onto user B's and applies B's limits.
        CurrentThread::attachToGroupIfDetached(borrowed);
        auto entry_b = process_list.insert(
            "INSERT INTO t VALUES", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context_b,
            /*watch_start_nanoseconds*/ 0, /*is_internal*/ true);

        /// Late async work of the flush, gated to run only after both queries left the process list.
        std::promise<void> queries_finished;
        std::shared_future<void> queries_finished_future = queries_finished.get_future().share();
        auto runner = threadPoolCallbackRunnerUnsafe<bool>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto allocation_threw = runner([queries_finished_future]
        {
            queries_finished_future.wait();

            constexpr Int64 allocation = 64 << 20;
            try
            {
                std::ignore = CurrentMemoryTracker::alloc(allocation);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    throw;
                return true;
            }
            std::ignore = CurrentMemoryTracker::free(allocation);
            return false;
        }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        /// Both queries leave the process list while the flush's async work is still pending.
        entry_b.reset();
        entry_a.reset();

        /// User A's next query lowers `max_memory_usage_for_user`. The pending flush work no longer
        /// charges user A, so it must not defer the reset of A's trackers - the lowered limit has to
        /// take effect immediately.
        auto context_a2 = Context::createCopy(getContext().context);
        context_a2->makeQueryContext();
        ClientInfo client_info_a2 = context_a2->getClientInfo();
        client_info_a2.current_user = "borrowed_scope_flushing_user";
        context_a2->setClientInfo(client_info_a2);
        context_a2->setCurrentQueryId("borrowed_scope_flushing_user_next_query");
        context_a2->setSetting("max_memory_usage_for_user", UInt64(16 << 20));
        context_a2->setSetting("memory_overcommit_ratio_denominator_for_user", UInt64(0));
        context_a2->setSetting("memory_usage_overcommit_max_wait_microseconds", UInt64(0));

        auto root_a2 = std::make_shared<ThreadGroup>(context_a2, 0);
        CurrentThread::attachToGroupIfDetached(root_a2);
        auto entry_a2 = process_list.insert(
            "SELECT 1", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context_a2,
            /*watch_start_nanoseconds*/ 0, /*is_internal*/ true);

        bool lowered_limit_enforced = false;
        constexpr Int64 allocation = 64 << 20;
        try
        {
            std::ignore = CurrentMemoryTracker::alloc(allocation);
            std::ignore = CurrentMemoryTracker::free(allocation);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                throw;
            lowered_limit_enforced = true;
        }
        EXPECT_TRUE(lowered_limit_enforced)
            << "a lowered `max_memory_usage_for_user` must take effect for the flushing user even "
               "while async work of a flush it triggered for another user is still running";

        entry_a2.reset();
        CurrentThread::detachFromGroupIfNotDetached();

        /// The flush's late async work must still obey the inserting user's limits.
        queries_finished.set_value();
        EXPECT_TRUE(allocation_threw.get())
            << "async allocation from a flush scope must still obey the inserting user's "
               "`max_memory_usage_for_user` after that user's last query left the process list";
        pool->wait();
    });
    t.join();
}

/// The `ProfileEvents` carrier must follow the same identity switch as the `memory_tracker`
/// carrier. When the flush registers its process-list entry for user B from inside user A's
/// query scope, only the flush group's own counters may be re-pointed at user B: rewiring the
/// OUTER query's counters (as a chain-walk up to the topmost process-level counters would do)
/// steals the wrapper query's later user-level `ProfileEvents` from user A, cross-contaminating
/// `system.user_processes`. Asserted through `ProcessList::getUserInfo`, the backing of
/// `system.user_processes`.
TEST(BorrowedThreadGroupLifetime, FlushForAnotherUserDoesNotStealFlushingUsersProfileEvents)
{
    std::thread t([&]
    {
        ThreadStatus ts;

        /// User A: runs the wrapper query that triggers the flush.
        auto context_a = Context::createCopy(getContext().context);
        context_a->makeQueryContext();
        ClientInfo client_info_a = context_a->getClientInfo();
        client_info_a.current_user = "borrowed_scope_profile_events_flushing_user";
        context_a->setClientInfo(client_info_a);
        context_a->setCurrentQueryId("borrowed_scope_profile_events_flushing_user_query");

        /// User B: owns the flushed inserts.
        auto context_b = Context::createCopy(getContext().context);
        context_b->makeQueryContext();
        ClientInfo client_info_b = context_b->getClientInfo();
        client_info_b.current_user = "borrowed_scope_profile_events_inserting_user";
        context_b->setClientInfo(client_info_b);
        context_b->setCurrentQueryId("borrowed_scope_profile_events_inserting_user_query");

        ProcessList process_list;

        /// User A's wrapper query, the outer scope the flush group is created from.
        auto root_a = std::make_shared<ThreadGroup>(context_a, 0);
        CurrentThread::attachToGroupIfDetached(root_a);
        auto entry_a = process_list.insert(
            "SYSTEM FLUSH ASYNC INSERT QUEUE", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context_a,
            /*watch_start_nanoseconds*/ 0, /*is_internal*/ true);

        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context_b, root_a);
        CurrentThread::detachFromGroupIfNotDetached();

        /// The flush registers its own process-list entry for user B - from here on the flush
        /// charges user B, for `ProfileEvents` exactly as for memory.
        CurrentThread::attachToGroupIfDetached(borrowed);
        auto entry_b = process_list.insert(
            "INSERT INTO t VALUES", /*normalized_query_hash*/ 0, /*ast*/ nullptr, context_b,
            /*watch_start_nanoseconds*/ 0, /*is_internal*/ true);

        constexpr ProfileEvents::Count flush_amount = 111;
        ProfileEvents::increment(ProfileEvents::SelectedRows, flush_amount);
        CurrentThread::detachFromGroupIfNotDetached();

        /// The wrapper query continues its own work as user A after the inner flush registered
        /// its entry.
        constexpr ProfileEvents::Count wrapper_amount = 10000;
        CurrentThread::attachToGroupIfDetached(root_a);
        ProfileEvents::increment(ProfileEvents::SelectedRows, wrapper_amount);
        CurrentThread::detachFromGroupIfNotDetached();

        auto user_info = process_list.getUserInfo(/*get_profile_events*/ true);
        const auto & counters_a = *user_info.at(client_info_a.current_user).profile_counters;
        const auto & counters_b = *user_info.at(client_info_b.current_user).profile_counters;
        EXPECT_EQ(counters_a[ProfileEvents::SelectedRows], wrapper_amount)
            << "the wrapper query's own `ProfileEvents` must keep counting towards the user that "
               "runs it after an inner flush registered a process-list entry for another user";
        EXPECT_EQ(counters_b[ProfileEvents::SelectedRows], flush_amount)
            << "the flush's `ProfileEvents` must count towards the user whose inserts are flushed, "
               "and only the flush's - not the outer wrapper query's";

        entry_b.reset();
        entry_a.reset();
    });
    t.join();
}

}
