#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ServerOwnedMemory.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB
{

namespace
{

/// Allocates in its constructor and frees in its destructor.
struct OwnsBuffer
{
    std::vector<char> buffer = std::vector<char>(8 << 20, 'x');
};

}

TEST(ServerOwnedMemory, NestedBlockerKeepsStrongerLevel)
{
    MemoryTrackerBlockerInThread global(VariableContext::Global);
    {
        ServerOwnedMemoryScope nested;
        EXPECT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
    }
    EXPECT_EQ(MemoryTrackerBlockerInThread::getLevel(), VariableContext::Global);
}

/// An object made by one query and released by another is charged to neither.
TEST(ServerOwnedMemory, MakeServerOwnedIsNotChargedToQueries)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        auto creator = std::make_shared<ThreadGroup>(context, 0);
        auto releaser = std::make_shared<ThreadGroup>(context, 0);

        CurrentThread::attachToGroupIfDetached(creator);
        auto object = makeServerOwned<OwnsBuffer>();
        CurrentThread::detachFromGroupIfNotDetached();

        CurrentThread::attachToGroupIfDetached(releaser);
        object.reset();
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_LT(std::abs(creator->memory_tracker.get()), 1 << 20) << "creator charged " << creator->memory_tracker.get();
        EXPECT_LT(std::abs(releaser->memory_tracker.get()), 1 << 20) << "releaser charged " << releaser->memory_tracker.get();
    });
    t.join();
}

/// What a job captured is released by the query that scheduled it, and the job wrapper is charged to neither.
TEST(ServerOwnedMemory, AttachedJobReleasesCapturesInQuery)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 10);
        auto group = std::make_shared<ThreadGroup>(context, 0);

        CurrentThread::attachToGroupIfDetached(group);
        for (size_t i = 0; i < 1000; ++i)
            pool.scheduleOrThrowOnError(attachedJob(ThreadName::UNKNOWN, [buffer = std::vector<char>(64 << 10, 'x')] {}));
        pool.wait();
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_LT(std::abs(group->memory_tracker.get()), 1 << 20) << "the query is left charged " << group->memory_tracker.get();
    });
    t.join();
}

/// A job dropped without running, here by a pool that shuts down after an exception, releases its captures in the query.
TEST(ServerOwnedMemory, AttachedJobDroppedUnrunReleasesCapturesInQuery)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;
        ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 10);
        auto group = std::make_shared<ThreadGroup>(context, 0);

        std::promise<void> started;
        std::promise<void> unblock;
        auto unblock_future = unblock.get_future().share();
        pool.scheduleOrThrowOnError([&started, unblock_future]
        {
            started.set_value();
            unblock_future.wait();
            throw std::runtime_error("shut the pool down");
        });
        started.get_future().wait();

        CurrentThread::attachToGroupIfDetached(group);
        pool.scheduleOrThrowOnError(attachedJob(ThreadName::UNKNOWN, [buffer = std::vector<char>(8 << 20, 'x')] {}));
        CurrentThread::detachFromGroupIfNotDetached();

        unblock.set_value();
        EXPECT_THROW(pool.wait(), std::runtime_error);

        EXPECT_LT(std::abs(group->memory_tracker.get()), 1 << 20) << "the query is left charged " << group->memory_tracker.get();
    });
    t.join();
}

}
