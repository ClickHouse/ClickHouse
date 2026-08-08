#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

/// Regression coverage for borrowed `ThreadGroup` async accounting.
///
/// Borrowed groups keep raw accounting pointers into the parent query group. They must stay scoped:
/// async captures must not extend borrowed accounting past the parent query lifetime.
///
/// The assertions below are deliberately written against observable behaviour (which group a pool
/// thread runs under, and whether a group stays alive) rather than against the internal predicates
/// introduced by the fix, so that this file also compiles against the unfixed sources - that is what
/// the `Bugfix validation (unit tests)` job needs in order to reproduce the bug.

namespace DB
{

namespace
{

std::unique_ptr<ThreadPool> makeSingleThreadPool()
{
    return std::make_unique<ThreadPool>(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        /*max_threads=*/ 1,
        /*max_free_threads=*/ 1,
        /*queue_size=*/ 10);
}

}

TEST(BorrowedThreadGroupLifetime, UnsafeRunnerEnqueuedUnderBorrowedGroupRunsWithoutIt)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        CurrentThread::attachToGroupIfDetached(borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<bool>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([borrowed] { return getCurrentThreadGroup() == borrowed; }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_FALSE(future.get()) << "async work must not run under a borrowed `ThreadGroup`";
        pool->wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, UnsafeRunnerEnqueuedUnderNestedBorrowedGroupRunsWithoutIt)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);
        CurrentThread::attachToGroupIfDetached(root);
        auto borrowed = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        /// A materialized view reading from another materialized view: the borrowed group of the inner
        /// view borrows from the borrowed group of the outer one, so it is borrowed just the same.
        CurrentThread::attachToGroupIfDetached(borrowed);
        auto nested_borrowed = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        CurrentThread::attachToGroupIfDetached(nested_borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<bool>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([nested_borrowed] { return getCurrentThreadGroup() == nested_borrowed; }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_FALSE(future.get()) << "async work must not run under a nested borrowed `ThreadGroup`";
        pool->wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, UnsafeRunnerEnqueuedUnderRootGroupKeepsIt)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);

        CurrentThread::attachToGroupIfDetached(root);
        auto runner = threadPoolCallbackRunnerUnsafe<bool>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([root] { return getCurrentThreadGroup() == root; }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_TRUE(future.get()) << "async work must still run under a normal (owning) `ThreadGroup`";
        pool->wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, UnsafeRunnerCreatedUnderRootGroupDoesNotKeepItAlive)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> root_weak = root;

        CurrentThread::attachToGroupIfDetached(root);
        auto runner = threadPoolCallbackRunnerUnsafe<void>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        CurrentThread::detachFromGroupIfNotDetached();

        root.reset();

        EXPECT_TRUE(root_weak.expired())
            << "a runner object must not keep the `ThreadGroup` current at its creation alive";
        pool->wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, LocalRunnerEnqueuedUnderBorrowedGroupRunsWithoutIt)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        CurrentThread::attachToGroupIfDetached(borrowed);
        ThreadPoolCallbackRunnerLocal<bool> runner(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto task = runner.enqueueAndGiveOwnership([borrowed] { return getCurrentThreadGroup() == borrowed; }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        ASSERT_TRUE(task->future.valid());
        EXPECT_FALSE(task->future.get()) << "async work must not run under a borrowed `ThreadGroup`";
        pool->wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, AsyncWorkFromBorrowedScopeKeepsCancellationContext)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        CurrentThread::attachToGroupIfDetached(borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<ThreadGroupPtr>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([] { return getCurrentThreadGroup(); }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        auto observed = future.get();
        pool->wait();

        /// The pool thread must not run under the borrowed group itself (raw pointers into the parent)...
        EXPECT_NE(observed, borrowed) << "async work must not run under a borrowed `ThreadGroup`";
        /// ...but it must still run under a group carrying the query's cancellation predicates: with no
        /// group at all, `CurrentThread::isQueryCanceled` / `checkIfNotCancelled` become no-ops on the
        /// pool thread (e.g. in the S3 client retry loop), so a canceled query would keep reading and
        /// retrying until the request finishes.
        ASSERT_NE(observed, nullptr) << "async work from a borrowed scope must keep a cancellation context";
        auto observed_shared_data = observed->getSharedData();
        ASSERT_TRUE(observed_shared_data.query_is_canceled_predicate);
        ASSERT_TRUE(observed_shared_data.throw_if_query_canceled_predicate);

        /// The predicates must stay safe to evaluate after the borrowed group and the parent query group
        /// are gone: async work may outlive the query.
        borrowed.reset();
        root.reset();
        EXPECT_FALSE(observed_shared_data.query_is_canceled_predicate());
        EXPECT_NO_THROW(observed_shared_data.throw_if_query_canceled_predicate());
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, AsyncWorkFromBorrowedScopeChargesOwningQueryAccounting)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> root_weak = root;
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        struct Result
        {
            ThreadGroupPtr observed;
            Int64 charged = 0;
        };

        CurrentThread::attachToGroupIfDetached(borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<Result>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([root]
        {
            /// Large enough to exceed any per-thread untracked-memory batching.
            constexpr Int64 allocation = 64 << 20;
            const Int64 before = root->memory_tracker.get();
            std::ignore = CurrentMemoryTracker::alloc(allocation);
            const Int64 charged = root->memory_tracker.get() - before;
            std::ignore = CurrentMemoryTracker::free(allocation);
            return Result{getCurrentThreadGroup(), charged};
        }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        auto result = future.get();
        pool->wait();

        /// The pool thread must not run under the borrowed group itself (raw pointers into the parent)...
        EXPECT_NE(result.observed, borrowed) << "async work must not run under a borrowed `ThreadGroup`";
        ASSERT_NE(result.observed, nullptr) << "async work from a borrowed scope must keep a group";

        /// ...but its allocations must still charge the owning query group: the query and user memory
        /// limits (`max_memory_usage` and alike) are wired onto the owning group by `ProcessList`, so
        /// accounting async work anywhere else would let it bypass them - and memory freed inside the
        /// callback would never be credited back to the query that paid for it.
        EXPECT_GE(result.charged, 32 << 20) << "async allocations must charge the owning query group";

        /// The async context must keep the owning group - its accounting target - alive: async work may
        /// outlive the query, and charging a freed group is a use-after-free.
        borrowed.reset();
        root.reset();
        EXPECT_FALSE(root_weak.expired())
            << "the async-callback group must keep the owning group it charges alive";
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, AsyncWorkFromNestedBorrowedScopeChargesUltimateOwner)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto root = std::make_shared<ThreadGroup>(context, 0);

        CurrentThread::attachToGroupIfDetached(root);
        auto borrowed = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        /// A materialized view reading from another materialized view: the inner borrowed group
        /// borrows from the outer borrowed group, but the accounting owner is still the query group.
        CurrentThread::attachToGroupIfDetached(borrowed);
        auto nested_borrowed = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        CurrentThread::attachToGroupIfDetached(nested_borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<Int64>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([root]
        {
            constexpr Int64 allocation = 64 << 20;
            const Int64 before = root->memory_tracker.get();
            std::ignore = CurrentMemoryTracker::alloc(allocation);
            const Int64 charged = root->memory_tracker.get() - before;
            std::ignore = CurrentMemoryTracker::free(allocation);
            return charged;
        }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_GE(future.get(), 32 << 20)
            << "async allocations from a nested borrowed scope must charge the owning query group";
        pool->wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, ChildDoesNotKeepParentAlive)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto parent = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> parent_weak = parent;

        /// Borrowed child: holds raw pointers into `parent->performance_counters` / `parent->memory_tracker`.
        CurrentThread::attachToGroupIfDetached(parent);
        auto child = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        /// Do not dereference the child after this point: it has raw accounting pointers into `parent`.
        parent.reset();

        EXPECT_TRUE(parent_weak.expired())
            << "Borrowed child must not keep its parent `ThreadGroup` alive";
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, FlushAsyncInsertQueueGroupDoesNotKeepParentAlive)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto parent = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> parent_weak = parent;

        /// Borrowed child via the public async-insert factory. Same borrowed counter pointers.
        auto child = ThreadGroup::createForFlushAsyncInsertQueue(context, parent);

        /// Do not dereference the child after this point: it has raw accounting pointers into `parent`.
        parent.reset();

        EXPECT_TRUE(parent_weak.expired())
            << "Async-insert borrowed child must not keep its parent `ThreadGroup` alive";
    });
    t.join();
}

} // namespace DB
