#include <Processors/Executors/Runtime/Pipeline/RemovalCoordinator.h>
#include <Processors/IProcessor.h>

#include <gtest/gtest.h>

#include <thread>
#include <vector>

using namespace DB;

namespace
{

class DummyProcessor final : public IProcessor
{
public:
    String getName() const override { return "Dummy"; }
    Status prepare() override { return Status::Finished; }
};

Processors makeProcessors(size_t count)
{
    Processors processors;
    for (size_t i = 0; i < count; ++i)
        processors.push_back(std::make_shared<DummyProcessor>());
    return processors;
}

std::vector<IProcessor *> pointers(const Processors & processors)
{
    std::vector<IProcessor *> result;
    for (const auto & processor : processors)
        result.push_back(processor.get());
    return result;
}

}

TEST(RemovalCoordinator, GroupBecomesReadyWhenEveryMemberFinished)
{
    auto processors = makeProcessors(3);
    auto ptrs = pointers(processors);

    RemovalCoordinator removals;
    removals.submit(processors);
    EXPECT_FALSE(removals.hasReady());

    removals.onFinished(ptrs[0]);
    removals.onFinished(ptrs[1]);
    EXPECT_FALSE(removals.hasReady());
    EXPECT_TRUE(removals.takeReadyForRemoval().empty());

    removals.onFinished(ptrs[2]);
    EXPECT_TRUE(removals.hasReady());

    auto taken = removals.takeReadyForRemoval();
    EXPECT_EQ(processors, taken);
    EXPECT_FALSE(removals.hasReady());
    EXPECT_TRUE(removals.takeReadyForRemoval().empty());
}

TEST(RemovalCoordinator, MembersFinishedBeforeSubmitAreCounted)
{
    auto processors = makeProcessors(2);
    auto ptrs = pointers(processors);

    RemovalCoordinator removals;
    removals.onFinished(ptrs[0]);
    removals.onFinished(ptrs[1]);
    EXPECT_FALSE(removals.hasReady());

    removals.submit(processors);
    EXPECT_TRUE(removals.hasReady());
    EXPECT_EQ(processors, removals.takeReadyForRemoval());
}

TEST(RemovalCoordinator, OnlyReadyGroupsAreTaken)
{
    auto first = makeProcessors(1);
    auto second = makeProcessors(2);
    auto second_ptrs = pointers(second);

    RemovalCoordinator removals;
    removals.submit(first);
    removals.submit(second);

    removals.onFinished(second_ptrs[0]);
    removals.onFinished(second_ptrs[1]);
    EXPECT_TRUE(removals.hasReady());
    EXPECT_EQ(second, removals.takeReadyForRemoval());
    EXPECT_FALSE(removals.hasReady());

    removals.onFinished(first.front().get());
    EXPECT_TRUE(removals.hasReady());
    EXPECT_EQ(first, removals.takeReadyForRemoval());
}

TEST(RemovalCoordinator, ConcurrentOnFinished)
{
    constexpr size_t threads_count = 8;
    constexpr size_t per_thread = 500;

    auto processors = makeProcessors(threads_count * per_thread);
    auto ptrs = pointers(processors);

    RemovalCoordinator removals;
    removals.submit(processors);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < threads_count; ++i)
    {
        threads.emplace_back([&, i]
        {
            for (size_t j = 0; j < per_thread; ++j)
                removals.onFinished(ptrs[i * per_thread + j]);
        });
    }

    for (auto & thread : threads)
        thread.join();

    EXPECT_TRUE(removals.hasReady());
    EXPECT_EQ(processors, removals.takeReadyForRemoval());
}
