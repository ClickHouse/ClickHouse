#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Processors/Executors/Runtime/Engine/TaskQueue.h>

#include <gtest/gtest.h>

#include <vector>

using namespace DB;

namespace
{

struct Fixture
{
    std::vector<ProcessorState> states;

    Fixture() : states(20) {}

    Task task(size_t i, Task::Kind kind = Task::Kind::Work) { return Task{.state = &states[i], .kind = kind}; }

    void fill(TaskQueue & queue, size_t from, size_t to)
    {
        for (size_t i = from; i < to; ++i)
            queue.pushBack(task(i));
    }

    void expectPops(TaskQueue & queue, std::vector<size_t> order)
    {
        for (size_t i : order)
        {
            ASSERT_FALSE(queue.empty());
            EXPECT_EQ(&states[i], queue.popFront().state);
        }
        EXPECT_TRUE(queue.empty());
    }
};

}

TEST(TaskQueue, PopFrontTakesTheOldestInOrder)
{
    Fixture f;
    TaskQueue queue;
    EXPECT_TRUE(queue.empty());

    queue.pushBack(f.task(0, Task::Kind::Prepare));
    queue.pushBack(f.task(1, Task::Kind::Work));
    queue.pushBack(f.task(2, Task::Kind::UpdatePipeline));
    EXPECT_EQ(3u, queue.size());

    Task first = queue.popFront();
    EXPECT_EQ(f.states.data(), first.state);
    EXPECT_EQ(Task::Kind::Prepare, first.kind);

    f.expectPops(queue, {1, 2});
}

TEST(TaskQueue, PopBackTakesTheNewest)
{
    Fixture f;
    TaskQueue queue;
    f.fill(queue, 0, 3);

    EXPECT_EQ(&f.states[2], queue.popBack().state);
    EXPECT_EQ(f.states.data(), queue.popFront().state);
    EXPECT_EQ(&f.states[1], queue.popBack().state);
    EXPECT_TRUE(queue.empty());
}

TEST(TaskQueue, PushFrontMakesTheOldest)
{
    Fixture f;
    TaskQueue queue;
    f.fill(queue, 0, 2);
    queue.pushFront(f.task(5));

    f.expectPops(queue, {5, 0, 1});
}

TEST(TaskQueue, TakeFrontMovesTheOldestBehindOwnTasks)
{
    Fixture f;
    TaskQueue from;
    TaskQueue to;
    f.fill(from, 0, 5);
    to.pushBack(f.task(7));

    to.takeFront(from, 3);
    f.expectPops(to, {7, 0, 1, 2});
    f.expectPops(from, {3, 4});
}

TEST(TaskQueue, TakeAllAppendsBehindOwnTasks)
{
    Fixture f;
    TaskQueue from;
    TaskQueue to;
    f.fill(from, 0, 3);
    to.pushBack(f.task(7));

    to.takeAll(from);
    EXPECT_TRUE(from.empty());
    f.expectPops(to, {7, 0, 1, 2});
}
