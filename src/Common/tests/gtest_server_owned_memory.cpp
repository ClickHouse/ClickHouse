#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ServerOwnedMemory.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

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

}
