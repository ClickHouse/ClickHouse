#include <future>
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

/// Assertions are written against observable behaviour (which group a pool thread runs under,
/// where allocations are charged, what stays alive), so this file also compiles against the
/// unfixed sources and reproduces the bugs there - that is what the `Bugfix validation
/// (unit tests)` job needs.

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

/// The group is captured when a task is enqueued, on the scheduling thread - not when the runner
/// object is created: a runner may be stored in a long-lived object (e.g. a `WriteBufferFromS3`
/// scheduler kept by a per-table writer) and reused by later queries, which would account all
/// their work to the query that happened to create it. Also pins that a runner object alone
/// keeps no group alive between enqueues.
TEST(ThreadPoolCallbackRunner, UnsafeRunnerCapturesGroupAtEnqueueTime)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto pool = makeSingleThreadPool();

        auto creation_group = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> creation_group_weak = creation_group;
        auto enqueue_group = std::make_shared<ThreadGroup>(context, 0);

        CurrentThread::attachToGroupIfDetached(creation_group);
        auto runner = threadPoolCallbackRunnerUnsafe<ThreadGroupPtr>(*pool, ThreadName::REMOTE_FS_READ_THREAD_POOL);
        CurrentThread::detachFromGroupIfNotDetached();

        CurrentThread::attachToGroupIfDetached(enqueue_group);
        auto future = runner([] { return getCurrentThreadGroup(); }, Priority{});
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_EQ(future.get(), enqueue_group)
            << "the pool thread must run under the group current at enqueue time, not at runner creation";
        pool->wait();

        creation_group.reset();
        EXPECT_TRUE(creation_group_weak.expired())
            << "a runner object must not keep the group current at its creation alive";
    });
    t.join();
}

} // namespace DB
