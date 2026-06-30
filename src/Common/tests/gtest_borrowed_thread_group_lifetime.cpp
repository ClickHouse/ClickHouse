#include <thread>

#include <gtest/gtest.h>

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

namespace DB
{

TEST(BorrowedThreadGroupLifetime, AsyncCallbackCaptureDropsBorrowedGroup)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto root = std::make_shared<ThreadGroup>(context, 0);
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        CurrentThread::attachToGroupIfDetached(borrowed);

        EXPECT_EQ(getCurrentThreadGroupForAsyncCallback(), nullptr);

        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, AsyncCallbackCaptureDropsNestedBorrowedGroup)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto root = std::make_shared<ThreadGroup>(context, 0);
        CurrentThread::attachToGroupIfDetached(root);
        auto borrowed = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        CurrentThread::attachToGroupIfDetached(borrowed);
        auto nested_borrowed = ThreadGroup::createForMaterializedView(context);
        CurrentThread::detachFromGroupIfNotDetached();

        CurrentThread::attachToGroupIfDetached(nested_borrowed);
        EXPECT_TRUE(nested_borrowed->isBorrowed());
        EXPECT_EQ(getCurrentThreadGroupForAsyncCallback(), nullptr);

        CurrentThread::detachFromGroupIfNotDetached();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, UnsafeRunnerCreatedUnderBorrowedGroupRunsWithoutBorrowedGroup)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        ThreadPool pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1,
            /*max_free_threads=*/ 1,
            /*queue_size=*/ 10);

        auto root = std::make_shared<ThreadGroup>(context, 0);
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        CurrentThread::attachToGroupIfDetached(borrowed);
        auto runner = threadPoolCallbackRunnerUnsafe<bool>(pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto future = runner([]
        {
            auto group = getCurrentThreadGroup();
            return group && group->isBorrowed();
        }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_FALSE(future.get());
        pool.wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, UnsafeRunnerCreatedUnderRootGroupDoesNotKeepItAlive)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        ThreadPool pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1,
            /*max_free_threads=*/ 1,
            /*queue_size=*/ 10);

        auto root = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> root_weak = root;

        CurrentThread::attachToGroupIfDetached(root);
        auto runner = threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        CurrentThread::detachFromGroupIfNotDetached();

        root.reset();

        EXPECT_TRUE(root_weak.expired());
        pool.wait();
    });
    t.join();
}

TEST(BorrowedThreadGroupLifetime, LocalRunnerCreatedUnderBorrowedGroupRunsWithoutBorrowedGroup)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        ThreadPool pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            /*max_threads=*/ 1,
            /*max_free_threads=*/ 1,
            /*queue_size=*/ 10);

        auto root = std::make_shared<ThreadGroup>(context, 0);
        auto borrowed = ThreadGroup::createForFlushAsyncInsertQueue(context, root);

        CurrentThread::attachToGroupIfDetached(borrowed);
        ThreadPoolCallbackRunnerLocal<bool> runner(pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        auto task = runner.enqueueAndGiveOwnership([]
        {
            auto group = getCurrentThreadGroup();
            return group && group->isBorrowed();
        }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        ASSERT_TRUE(task->future.valid());
        EXPECT_FALSE(task->future.get());
        pool.wait();
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
