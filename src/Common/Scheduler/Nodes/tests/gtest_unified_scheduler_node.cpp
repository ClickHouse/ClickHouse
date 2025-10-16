#include <chrono>
#include <gtest/gtest.h>

#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Common/Priority.h>
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

TEST(SchedulerUnifiedNode, ThrottlerLeakyBucket)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    auto all = t.createUnifiedNode("all", {.priority = Priority{}, .max_speed = 10.0, .max_burst = 20.0});

    t.enqueue(all, {10, 10, 10, 10, 10, 10, 10, 10});

    t.process(start + std::chrono::seconds(0));
    t.consumed("all", 30); // It is allowed to go below zero for exactly one resource request

    t.process(start + std::chrono::seconds(1));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(2));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(3));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(4));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(100500));
    t.consumed("all", 10);
}

TEST(SchedulerUnifiedNode, ThrottlerPacing)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    // Zero burst allows you to send one request of any `size` and than throttle for `size/max_speed` seconds.
    // Useful if outgoing traffic should be "paced", i.e. have the least possible burstiness.
    auto all = t.createUnifiedNode("all", {.priority = Priority{}, .max_speed = 1.0, .max_burst = 0.0});

    t.enqueue(all, {1, 2, 3, 1, 2, 1});
    int output[] = {1, 2, 0, 3, 0, 0, 1, 2, 0, 1, 0};
    for (int i = 0; i < std::size(output); i++)
    {
        t.process(start + std::chrono::seconds(i));
        t.consumed("all", output[i]);
    }
}

TEST(SchedulerUnifiedNode, ThrottlerBucketFilling)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    auto all = t.createUnifiedNode("all", {.priority = Priority{}, .max_speed = 10.0, .max_burst = 100.0});

    t.enqueue(all, {100});

    t.process(start + std::chrono::seconds(0));
    t.consumed("all", 100); // consume all tokens, but it is still active (not negative)

    t.process(start + std::chrono::seconds(5));
    t.consumed("all", 0); // There was nothing to consume

    t.enqueue(all, {10, 10, 10, 10, 10, 10, 10, 10, 10, 10});
    t.process(start + std::chrono::seconds(5));
    t.consumed("all", 60); // 5 sec * 10 tokens/sec = 50 tokens + 1 extra request to go below zero

    t.process(start + std::chrono::seconds(100));
    t.consumed("all", 40); // Consume rest

    t.process(start + std::chrono::seconds(200));

    t.enqueue(all, {95, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    t.process(start + std::chrono::seconds(200));
    t.consumed("all", 101); // check we cannot consume more than max_burst + 1 request

    t.process(start + std::chrono::seconds(100500));
    t.consumed("all", 3);
}

TEST(SchedulerUnifiedNode, ThrottlerAndFairness)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    auto all = t.createUnifiedNode("all", {.priority = Priority{}, .max_speed = 10.0, .max_burst = 100.0});
    auto a = t.createUnifiedNode("A", all, {.weight = 10.0, .priority = Priority{}});
    auto b = t.createUnifiedNode("B", all, {.weight = 90.0, .priority = Priority{}});

    ResourceCost req_cost = 1;
    ResourceCost total_cost = 2000;
    for (int i = 0; i < total_cost / req_cost; i++)
    {
        t.enqueue(a, {req_cost});
        t.enqueue(b, {req_cost});
    }

    double share_a = 0.1;
    double share_b = 0.9;

    // Bandwidth-latency coupling due to fairness: worst latency is inversely proportional to share
    auto max_latency_a = static_cast<ResourceCost>(req_cost * (1.0 + 1.0 / share_a));
    auto max_latency_b = static_cast<ResourceCost>(req_cost * (1.0 + 1.0 / share_b));

    double consumed_a = 0;
    double consumed_b = 0;
    for (int seconds = 0; seconds < 100; seconds++)
    {
        t.process(start + std::chrono::seconds(seconds));
        double arrival_curve = 100.0 + 10.0 * seconds + req_cost;
        t.consumed("A", static_cast<ResourceCost>(arrival_curve * share_a - consumed_a), max_latency_a);
        t.consumed("B", static_cast<ResourceCost>(arrival_curve * share_b - consumed_b), max_latency_b);
        consumed_a = arrival_curve * share_a;
        consumed_b = arrival_curve * share_b;
    }
}

TEST(SchedulerUnifiedNode, QueueWithRequestsDestruction)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");

    t.enqueue(all, {10, 10}); // enqueue reqeuests to be canceled

    // This will destroy the queue and fail both requests
    auto a = t.createUnifiedNode("A", all);
    t.failed(20);

    // Check that everything works fine after destruction
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

TEST(SchedulerUnifiedNode, ResourceGuardException)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");

    t.enqueue(all, {10, 10}); // enqueue reqeuests to be canceled

    std::thread consumer([queue = all->getQueue()]
    {
        ResourceLink link{.queue = queue.get()};
        bool caught = false;
        try
        {
            ResourceGuard rg(ResourceGuard::Metrics::getIOWrite(), link);
        }
        catch (...)
        {
            caught = true;
        }
        ASSERT_TRUE(caught);
    });

    // This will destroy the queue and fail both requests
    auto a = t.createUnifiedNode("A", all);
    t.failed(20);
    consumer.join();

    // Check that everything works fine after destruction
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

TEST(SchedulerUnifiedNode, UpdateWeight)
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

    t.updateUnifiedNode(b, all, all, {.weight = 1.0, .priority = Priority{}});

    t.dequeue(4);
    t.consumed("A", 20);
    t.consumed("B", 20);

    t.dequeue(4);
    t.consumed("A", 20);
    t.consumed("B", 20);
}

TEST(SchedulerUnifiedNode, UpdatePriority)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.weight = 1.0, .priority = Priority{}});
    auto b = t.createUnifiedNode("B", all, {.weight = 1.0, .priority = Priority{}});

    t.enqueue(a, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(b, {10, 10, 10, 10, 10, 10, 10, 10});

    t.dequeue(2);
    t.consumed("A", 10);
    t.consumed("B", 10);

    t.updateUnifiedNode(a, all, all, {.weight = 1.0, .priority = Priority{-1}});

    t.dequeue(2);
    t.consumed("A", 20);
    t.consumed("B", 0);

    t.updateUnifiedNode(b, all, all, {.weight = 1.0, .priority = Priority{-2}});

    t.dequeue(2);
    t.consumed("A", 0);
    t.consumed("B", 20);

    t.updateUnifiedNode(a, all, all, {.weight = 1.0, .priority = Priority{-2}});

    t.dequeue(2);
    t.consumed("A", 10);
    t.consumed("B", 10);
}

TEST(SchedulerUnifiedNode, UpdateParentOfLeafNode)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.weight = 1.0, .priority = Priority{1}});
    auto b = t.createUnifiedNode("B", all, {.weight = 1.0, .priority = Priority{2}});
    auto x = t.createUnifiedNode("X", a, {});
    auto y = t.createUnifiedNode("Y", b, {});

    t.enqueue(x, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(y, {10, 10, 10, 10, 10, 10, 10, 10});

    t.dequeue(2);
    t.consumed("X", 20);
    t.consumed("Y", 0);

    t.updateUnifiedNode(x, a, b, {});

    t.dequeue(2);
    t.consumed("X", 10);
    t.consumed("Y", 10);

    t.updateUnifiedNode(y, b, a, {});

    t.dequeue(2);
    t.consumed("X", 0);
    t.consumed("Y", 20);

    t.updateUnifiedNode(y, a, all, {});
    t.updateUnifiedNode(x, b, all, {});

    t.dequeue(4);
    t.consumed("X", 20);
    t.consumed("Y", 20);
}

TEST(SchedulerUnifiedNode, UpdatePriorityOfIntermediateNode)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.weight = 1.0, .priority = Priority{1}});
    auto b = t.createUnifiedNode("B", all, {.weight = 1.0, .priority = Priority{2}});
    auto x1 = t.createUnifiedNode("X1", a, {});
    auto y1 = t.createUnifiedNode("Y1", b, {});
    auto x2 = t.createUnifiedNode("X2", a, {});
    auto y2 = t.createUnifiedNode("Y2", b, {});

    t.enqueue(x1, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(y1, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(x2, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(y2, {10, 10, 10, 10, 10, 10, 10, 10});

    t.dequeue(4);
    t.consumed("X1", 20);
    t.consumed("Y1", 0);
    t.consumed("X2", 20);
    t.consumed("Y2", 0);

    t.updateUnifiedNode(a, all, all, {.weight = 1.0, .priority = Priority{2}});

    t.dequeue(4);
    t.consumed("X1", 10);
    t.consumed("Y1", 10);
    t.consumed("X2", 10);
    t.consumed("Y2", 10);

    t.updateUnifiedNode(b, all, all, {.weight = 1.0, .priority = Priority{1}});

    t.dequeue(4);
    t.consumed("X1", 0);
    t.consumed("Y1", 20);
    t.consumed("X2", 0);
    t.consumed("Y2", 20);
}

TEST(SchedulerUnifiedNode, UpdateParentOfIntermediateNode)
{
    ResourceTest t;

    auto all = t.createUnifiedNode("all");
    auto a = t.createUnifiedNode("A", all, {.weight = 1.0, .priority = Priority{1}});
    auto b = t.createUnifiedNode("B", all, {.weight = 1.0, .priority = Priority{2}});
    auto c = t.createUnifiedNode("C", a, {});
    auto d = t.createUnifiedNode("D", b, {});
    auto x1 = t.createUnifiedNode("X1", c, {});
    auto y1 = t.createUnifiedNode("Y1", d, {});
    auto x2 = t.createUnifiedNode("X2", c, {});
    auto y2 = t.createUnifiedNode("Y2", d, {});

    t.enqueue(x1, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(y1, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(x2, {10, 10, 10, 10, 10, 10, 10, 10});
    t.enqueue(y2, {10, 10, 10, 10, 10, 10, 10, 10});

    t.dequeue(4);
    t.consumed("X1", 20);
    t.consumed("Y1", 0);
    t.consumed("X2", 20);
    t.consumed("Y2", 0);

    t.updateUnifiedNode(c, a, b, {});

    t.dequeue(4);
    t.consumed("X1", 10);
    t.consumed("Y1", 10);
    t.consumed("X2", 10);
    t.consumed("Y2", 10);

    t.updateUnifiedNode(d, b, a, {});

    t.dequeue(4);
    t.consumed("X1", 0);
    t.consumed("Y1", 20);
    t.consumed("X2", 0);
    t.consumed("Y2", 20);
}

TEST(SchedulerUnifiedNode, UpdateThrottlerMaxSpeed)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    auto all = t.createUnifiedNode("all", {.priority = Priority{}, .max_speed = 10.0, .max_burst = 20.0});

    t.enqueue(all, {10, 10, 10, 10, 10, 10, 10, 10});

    t.process(start + std::chrono::seconds(0));
    t.consumed("all", 30); // It is allowed to go below zero for exactly one resource request

    t.process(start + std::chrono::seconds(1));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(2));
    t.consumed("all", 10);

    t.updateUnifiedNode(all, {}, {}, {.priority = Priority{}, .max_speed = 1.0, .max_burst = 20.0});

    t.process(start + std::chrono::seconds(12));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(22));
    t.consumed("all", 10);

    t.process(start + std::chrono::seconds(100500));
    t.consumed("all", 10);
}

TEST(SchedulerUnifiedNode, UpdateThrottlerMaxBurst)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    auto all = t.createUnifiedNode("all", {.priority = Priority{}, .max_speed = 10.0, .max_burst = 100.0});

    t.enqueue(all, {100});

    t.process(start + std::chrono::seconds(0));
    t.consumed("all", 100); // consume all tokens, but it is still active (not negative)

    t.process(start + std::chrono::seconds(2));
    t.consumed("all", 0); // There was nothing to consume
    t.updateUnifiedNode(all, {}, {}, {.priority = Priority{}, .max_speed = 10.0, .max_burst = 30.0});

    t.process(start + std::chrono::seconds(5));
    t.consumed("all", 0); // There was nothing to consume

    t.enqueue(all, {10, 10, 10, 10, 10, 10, 10, 10, 10, 10});
    t.process(start + std::chrono::seconds(5));
    t.consumed("all", 40); // min(30 tokens, 5 sec * 10 tokens/sec) = 30 tokens + 1 extra request to go below zero

    t.updateUnifiedNode(all, {}, {}, {.priority = Priority{}, .max_speed = 10.0, .max_burst = 100.0});

    t.process(start + std::chrono::seconds(100));
    t.consumed("all", 60); // Consume rest

    t.process(start + std::chrono::seconds(150));
    t.updateUnifiedNode(all, {}, {}, {.priority = Priority{}, .max_speed = 100.0, .max_burst = 200.0});

    t.process(start + std::chrono::seconds(200));

    t.enqueue(all, {195, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    t.process(start + std::chrono::seconds(200));
    t.consumed("all", 201); // check we cannot consume more than max_burst + 1 request

    t.process(start + std::chrono::seconds(100500));
    t.consumed("all", 3);
}
