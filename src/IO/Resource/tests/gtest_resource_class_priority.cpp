#include <gtest/gtest.h>

#include <IO/Resource/tests/ResourceTest.h>

#include <IO/Resource/PriorityPolicy.h>

using namespace DB;

using ResourceTest = ResourceTestClass;

TEST(IOResourcePriorityPolicy, Factory)
{
    ResourceTest t;

    Poco::AutoPtr cfg = new Poco::Util::XMLConfiguration();
    SchedulerNodePtr prio = SchedulerNodeFactory::instance().get("priority", /* event_queue = */ nullptr, *cfg, "");
    EXPECT_TRUE(dynamic_cast<PriorityPolicy *>(prio.get()) != nullptr);
}

TEST(IOResourcePriorityPolicy, Priorities)
{
    ResourceTest t;

    t.add<PriorityPolicy>("/");
    t.add<FifoQueue>("/A", "<priority>3</priority>");
    t.add<FifoQueue>("/B", "<priority>2</priority>");
    t.add<FifoQueue>("/C", "<priority>1</priority>");

    t.enqueue("/A", {10, 10, 10});
    t.enqueue("/B", {10, 10, 10});
    t.enqueue("/C", {10, 10, 10});

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

TEST(IOResourcePriorityPolicy, Activation)
{
    ResourceTest t;

    t.add<PriorityPolicy>("/");
    t.add<FifoQueue>("/A", "<priority>3</priority>");
    t.add<FifoQueue>("/B", "<priority>2</priority>");
    t.add<FifoQueue>("/C", "<priority>1</priority>");

    t.enqueue("/A", {10, 10, 10, 10, 10, 10});
    t.enqueue("/B", {10});
    t.enqueue("/C", {10, 10});

    t.dequeue(3);
    t.consumed("A", 0);
    t.consumed("B", 10);
    t.consumed("C", 20);

    t.dequeue(2);
    t.consumed("A", 20);
    t.consumed("B", 0);
    t.consumed("C", 0);

    t.enqueue("/B", {10, 10, 10});
    t.dequeue(2);
    t.consumed("A", 0);
    t.consumed("B", 20);
    t.consumed("C", 0);

    t.enqueue("/C", {10, 10});
    t.dequeue(3);
    t.consumed("A", 0);
    t.consumed("B", 10);
    t.consumed("C", 20);

    t.dequeue(2);
    t.consumed("A", 20);
    t.consumed("B", 0);
    t.consumed("C", 0);
}

TEST(IOResourcePriorityPolicy, SinglePriority)
{
    ResourceTest t;

    t.add<PriorityPolicy>("/");
    t.add<FifoQueue>("/A");

    for (int i = 0; i < 3; i++)
    {
        t.enqueue("/A", {10, 10});
        t.dequeue(1);
        t.consumed("A", 10);

        for (int j = 0; j < 3; j++)
        {
            t.enqueue("/A", {10, 10, 10});
            t.dequeue(1);
            t.consumed("A", 10);
            t.dequeue(1);
            t.consumed("A", 10);
            t.dequeue(1);
            t.consumed("A", 10);
        }

        t.dequeue(1);
        t.consumed("A", 10);
    }
}
