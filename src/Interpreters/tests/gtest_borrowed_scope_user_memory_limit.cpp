#include <future>
#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
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

}
