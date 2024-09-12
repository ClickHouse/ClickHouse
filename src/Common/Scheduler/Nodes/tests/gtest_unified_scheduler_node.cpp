#include <chrono>
#include <gtest/gtest.h>

#include "Common/Priority.h"
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Common/Scheduler/Nodes/FairPolicy.h>
#include <Common/Scheduler/Nodes/UnifiedSchedulerNode.h>

using namespace DB;

using ResourceTest = ResourceTestClass;

TEST(SchedulerUnifiedNode, Smoke)
{
    ResourceTest t;

    t.addCustom<UnifiedSchedulerNode>("/", SchedulingSettings{});

    t.enqueue("/fifo", {10, 10});
    t.dequeue(2);
    t.consumed("fifo", 20);
}

TEST(SchedulerUnifiedNode, FairnessWeight)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.weight = 1.0, .priority = Priority{}});
    auto b = t.createUnifiedNode("B", all, {.weight = 3.0, .priority = Priority{}});

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});

    t.dequeue(4);
    t.consumed("A", 10);
    t.consumed("B", 30);

    t.dequeue(4);
    t.consumed("A", 10);
    t.consumed("B", 30);

    t.dequeue();
    t.consumed("A", 60);
    t.consumed("B", 20);
}

TEST(SchedulerUnifiedNode, FairnessActivation)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all);
    auto b = t.createUnifiedNode("B", all);
    auto c = t.createUnifiedNode("C", all);

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10});
    t.enqueue(c, {10, 10});

    t.dequeue(3);
    t.consumed("A", 10);
    t.consumed("B", 10);
    t.consumed("C", 10);

    t.dequeue(4);
    t.consumed("A", 30);
    t.consumed("B", 0);
    t.consumed("C", 10);

    t.enqueue(b, {10, 10});
    t.dequeue(1);
    t.consumed("B", 10);

    t.enqueue(c, {10, 10});
    t.dequeue(1);
    t.consumed("C", 10);

    t.dequeue(2); // A B or B A
    t.consumed("A", 10);
    t.consumed("B", 10);
}

TEST(SchedulerUnifiedNode, FairnessMaxMin)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all);
    auto b = t.createUnifiedNode("B", all);

    t.enqueue(a, {10, 10}); // make sure A is never empty

    for (int i = 0; i < 10; i++)
    {
        t.enqueue(a, {10, 10, 10, 10});
        t.enqueue(b, {10, 10});

        t.dequeue(6);
        t.consumed("A", 40);
        t.consumed("B", 20);
    }

    t.dequeue(2);
    t.consumed("A", 20);
}

TEST(SchedulerUnifiedNode, FairnessHierarchical)
{
    ResourceTest t;


    auto all = t.createUnifiedNode("all");
    auto x = t.createUnifiedNode("X", all);
    auto y = t.createUnifiedNode("Y", all);
    auto a = t.createUnifiedNode("A", x);
    auto b = t.createUnifiedNode("B", x);
    auto c = t.createUnifiedNode("C", y);
    auto d = t.createUnifiedNode("D", y);

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(c, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 20);
        t.consumed("B", 20);
        t.consumed("C", 20);
        t.consumed("D", 20);
    }

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(c, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 40);
        t.consumed("C", 20);
        t.consumed("D", 20);
    }

    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(c, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("B", 40);
        t.consumed("C", 20);
        t.consumed("D", 20);
    }

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(c, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(c, {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 20);
        t.consumed("B", 20);
        t.consumed("C", 40);
    }

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 20);
        t.consumed("B", 20);
        t.consumed("D", 40);
    }

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(d, {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 40);
        t.consumed("D", 40);
    }
}

TEST(SchedulerUnifiedNode, Priority)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.priority = Priority{3}});
    auto b = t.createUnifiedNode("B", all, {.priority = Priority{2}});
    auto c = t.createUnifiedNode("C", all, {.priority = Priority{1}});

    t.enqueue(a, {10, 10, 10});
    t.enqueue(b, {10, 10, 10});
    t.enqueue(c, {10, 10, 10});

    t.dequeue(2);
    t.consumed("A", 0);
    t.consumed("B", 0);
    t.consumed("C", 20);

    t.dequeue(2);
    t.consumed("A", 0);
    t.consumed("B", 10);
    t.consumed("C", 10);

    t.dequeue(2);
    t.consumed("A", 0);
    t.consumed("B", 20);
    t.consumed("C", 0);

    t.dequeue();
    t.consumed("A", 30);
    t.consumed("B", 0);
    t.consumed("C", 0);
}

TEST(SchedulerUnifiedNode, PriorityActivation)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.priority = Priority{3}});
    auto b = t.createUnifiedNode("B", all, {.priority = Priority{2}});
    auto c = t.createUnifiedNode("C", all, {.priority = Priority{1}});

    t.enqueue(a, {10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10});
    t.enqueue(c, {10, 10});

    t.dequeue(3);
    t.consumed("A", 0);
    t.consumed("B", 10);
    t.consumed("C", 20);

    t.dequeue(2);
    t.consumed("A", 20);
    t.consumed("B", 0);
    t.consumed("C", 0);

    t.enqueue(b, {10, 10, 10});
    t.dequeue(2);
    t.consumed("A", 0);
    t.consumed("B", 20);
    t.consumed("C", 0);

    t.enqueue(c, {10, 10});
    t.dequeue(3);
    t.consumed("A", 0);
    t.consumed("B", 10);
    t.consumed("C", 20);

    t.dequeue(2);
    t.consumed("A", 20);
    t.consumed("B", 0);
    t.consumed("C", 0);
}

TEST(SchedulerUnifiedNode, List)
{
    ResourceTest t;

    std::list<UnifiedSchedulerNodePtr> list;
    list.push_back(t.createUnifiedNode("all"));

    for (int length = 1; length < 5; length++)
    {
        String name = fmt::format("L{}", length);
        list.push_back(t.createUnifiedNode(name, list.back()));

        for (int i = 0; i < 3; i++)
        {
            t.enqueue(list.back(), {10, 10});
            t.dequeue(1);
            t.consumed(name, 10);

            for (int j = 0; j < 3; j++)
            {
                t.enqueue(list.back(), {10, 10, 10});
                t.dequeue(1);
                t.consumed(name, 10);
                t.dequeue(1);
                t.consumed(name, 10);
                t.dequeue(1);
                t.consumed(name, 10);
            }

            t.dequeue(1);
            t.consumed(name, 10);
        }
    }
}
