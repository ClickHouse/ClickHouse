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

TEST(SchedulerUnifiedNode, Fairness)
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
