#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Common/Scheduler/Nodes/FairPolicy.h>

using namespace DB;

using ResourceTest = ResourceTestClass;

/// Tests disabled because of leaks in the test themselves: https://github.com/ClickHouse/ClickHouse/issues/67678

TEST(DISABLED_SchedulerFairPolicy, Factory)
{
    ResourceTest t;

    Poco::AutoPtr cfg = new Poco::Util::XMLConfiguration();
    SchedulerNodePtr fair = SchedulerNodeFactory::instance().get("fair", /* event_queue = */ nullptr, *cfg, "");
    EXPECT_TRUE(dynamic_cast<FairPolicy *>(fair.get()) != nullptr);
}

TEST(DISABLED_SchedulerFairPolicy, FairnessWeights)
{
    ResourceTest t;

    t.add<FairPolicy>("/");
    t.add<FifoQueue>("/A", "<weight>1.0</weight>");
    t.add<FifoQueue>("/B", "<weight>3.0</weight>");

    t.enqueue("/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/B", {10, 10, 10, 10, 10, 10, 10, 10});

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

TEST(DISABLED_SchedulerFairPolicy, Activation)
{
    ResourceTest t;

    t.add<FairPolicy>("/");
    t.add<FifoQueue>("/A");
    t.add<FifoQueue>("/B");
    t.add<FifoQueue>("/C");

    t.enqueue("/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/B", {10});
    t.enqueue("/C", {10, 10});

    t.dequeue(3);
    t.consumed("A", 10);
    t.consumed("B", 10);
    t.consumed("C", 10);

    t.dequeue(4);
    t.consumed("A", 30);
    t.consumed("B", 0);
    t.consumed("C", 10);

    t.enqueue("/B", {10, 10});
    t.dequeue(1);
    t.consumed("B", 10);

    t.enqueue("/C", {10, 10});
    t.dequeue(1);
    t.consumed("C", 10);

    t.dequeue(2); // A B or B A
    t.consumed("A", 10);
    t.consumed("B", 10);
}

TEST(DISABLED_SchedulerFairPolicy, FairnessMaxMin)
{
    ResourceTest t;

    t.add<FairPolicy>("/");
    t.add<FifoQueue>("/A");
    t.add<FifoQueue>("/B");

    t.enqueue("/A", {10, 10}); // make sure A is never empty

    for (int i = 0; i < 10; i++)
    {
        t.enqueue("/A", {10, 10, 10, 10});
        t.enqueue("/B", {10, 10});

        t.dequeue(6);
        t.consumed("A", 40);
        t.consumed("B", 20);
    }

    t.dequeue(2);
    t.consumed("A", 20);
}

TEST(DISABLED_SchedulerFairPolicy, HierarchicalFairness)
{
    ResourceTest t;

    t.add<FairPolicy>("/");
    t.add<FairPolicy>("/X");
    t.add<FairPolicy>("/Y");
    t.add<FifoQueue>("/X/A");
    t.add<FifoQueue>("/X/B");
    t.add<FifoQueue>("/Y/C");
    t.add<FifoQueue>("/Y/D");

    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/X/B", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/C", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 20);
        t.consumed("B", 20);
        t.consumed("C", 20);
        t.consumed("D", 20);
    }

    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/C", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 40);
        t.consumed("C", 20);
        t.consumed("D", 20);
    }

    t.enqueue("/X/B", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/X/B", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/C", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("B", 40);
        t.consumed("C", 20);
        t.consumed("D", 20);
    }

    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/X/B", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/C", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/C", {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 20);
        t.consumed("B", 20);
        t.consumed("C", 40);
    }

    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/X/B", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 20);
        t.consumed("B", 20);
        t.consumed("D", 40);
    }

    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/X/A", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue("/Y/D", {10, 10, 10, 10, 10, 10, 10, 10});
    for (int i = 0; i < 4; i++)
    {
        t.dequeue(8);
        t.consumed("A", 40);
        t.consumed("D", 40);
    }
}
