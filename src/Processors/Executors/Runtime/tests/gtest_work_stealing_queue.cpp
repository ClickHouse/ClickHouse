#include <Processors/Executors/Runtime/Engine/State/ProcessorState.h>
#include <Processors/Executors/Runtime/Engine/WorkStealingQueue.h>
#include <Common/Exception.h>

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

    void fill(WorkStealingQueue & deque, size_t from, size_t to)
    {
        for (size_t i = from; i < to; ++i)
            deque.push(task(i));
    }

    void expectPops(WorkStealingQueue & deque, std::vector<size_t> order)
    {
        for (size_t i : order)
        {
            ASSERT_FALSE(deque.empty());
            EXPECT_EQ(&states[i], deque.pop().state);
        }
        EXPECT_TRUE(deque.empty());
    }
};

}

TEST(WorkStealingQueue, OwnerPopsInPushOrder)
{
    Fixture f;
    WorkStealingQueue deque;
    EXPECT_TRUE(deque.empty());
    EXPECT_THROW(deque.pop(), Exception);

    deque.push(f.task(0, Task::Kind::Prepare));
    deque.push(f.task(1, Task::Kind::Work));
    deque.push(f.task(2, Task::Kind::UpdatePipeline));
    EXPECT_EQ(3u, deque.size());

    Task first = deque.pop();
    EXPECT_EQ(&f.states[0], first.state);
    EXPECT_EQ(Task::Kind::Prepare, first.kind);

    f.expectPops(deque, {1, 2});
    EXPECT_THROW(deque.pop(), Exception);
}

TEST(WorkStealingQueue, StealTakesNewestHalfInOrder)
{
    Fixture f;
    WorkStealingQueue victim;
    WorkStealingQueue thief;
    f.fill(victim, 0, 6);

    EXPECT_EQ(3u, thief.stealFrom(victim));
    EXPECT_EQ(3u, thief.size());
    EXPECT_EQ(3u, victim.size());

    f.expectPops(thief, {3, 4, 5});
    f.expectPops(victim, {0, 1, 2});
}

TEST(WorkStealingQueue, StealRoundsUpAndAppendsBehindOwnTasks)
{
    Fixture f;
    WorkStealingQueue victim;
    WorkStealingQueue thief;
    f.fill(victim, 0, 5);
    thief.push(f.task(7));

    EXPECT_EQ(3u, thief.stealFrom(victim));
    f.expectPops(thief, {7, 2, 3, 4});
    f.expectPops(victim, {0, 1});
}

TEST(WorkStealingQueue, StealIsCappedByMaxCount)
{
    Fixture f;
    WorkStealingQueue victim;
    WorkStealingQueue thief;
    f.fill(victim, 0, 20);

    EXPECT_EQ(7u, thief.stealFrom(victim));
    f.expectPops(thief, {13, 14, 15, 16, 17, 18, 19});

    EXPECT_EQ(2u, thief.stealFrom(victim, 2));
    f.expectPops(thief, {11, 12});
    f.expectPops(victim, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
}

TEST(WorkStealingQueue, StealFromSingleAndEmpty)
{
    Fixture f;
    WorkStealingQueue victim;
    WorkStealingQueue thief;

    EXPECT_EQ(0u, thief.stealFrom(victim));
    EXPECT_TRUE(thief.empty());

    victim.push(f.task(0));
    EXPECT_EQ(1u, thief.stealFrom(victim));
    EXPECT_TRUE(victim.empty());
    f.expectPops(thief, {0});

    EXPECT_THROW(thief.stealFrom(thief), Exception);
}
