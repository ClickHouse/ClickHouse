#include <chrono>
#include <gtest/gtest.h>

#include <Common/Scheduler/Nodes/tests/ResourceTest.h>

#include <Common/Scheduler/Nodes/FairPolicy.h>
#include <Common/Scheduler/Nodes/ThrottlerConstraint.h>

using namespace DB;

using ResourceTest = ResourceTestClass;

TEST(SchedulerThrottlerConstraint, LeakyBucketConstraint)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    t.add<ThrottlerConstraint>("/", "<max_burst>20.0</max_burst><max_speed>10.0</max_speed>");
    t.add<FifoQueue>("/A", "");

    t.enqueue("/A", {10, 10, 10, 10, 10, 10, 10, 10});

    t.process(start + std::chrono::seconds(0));
    t.consumed("A", 30); // It is allowed to go below zero for exactly one resource request

    t.process(start + std::chrono::seconds(1));
    t.consumed("A", 10);

    t.process(start + std::chrono::seconds(2));
    t.consumed("A", 10);

    t.process(start + std::chrono::seconds(3));
    t.consumed("A", 10);

    t.process(start + std::chrono::seconds(4));
    t.consumed("A", 10);

    t.process(start + std::chrono::seconds(100500));
    t.consumed("A", 10);
}

TEST(SchedulerThrottlerConstraint, Unlimited)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    t.add<ThrottlerConstraint>("/", "");
    t.add<FifoQueue>("/A", "");

    for (int i = 0; i < 10; i++)
    {
        t.enqueue("/A", {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000});
        t.process(start + std::chrono::seconds(i / 2)); // Stick to the same time twice
        t.consumed("A", 11111111);
    }
}

TEST(SchedulerThrottlerConstraint, Pacing)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    // Zero burst allows you to send one request of any `size` and than throttle for `size/max_speed` seconds.
    // Useful if outgoing traffic should be "paced", i.e. have the least possible burstiness.
    t.add<ThrottlerConstraint>("/", "<max_burst>0</max_burst><max_speed>1</max_speed>");
    t.add<FifoQueue>("/A", "");

    t.enqueue("/A", {1, 2, 3, 1, 2, 1});
    int output[] = {1, 2, 0, 3, 0, 0, 1, 2, 0, 1, 0};
    for (int i = 0; i < std::size(output); i++)
    {
        t.process(start + std::chrono::seconds(i));
        t.consumed("A", output[i]);
    }
}

TEST(SchedulerThrottlerConstraint, BucketFilling)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    t.add<ThrottlerConstraint>("/", "<max_burst>100.0</max_burst><max_speed>10.0</max_speed>");
    t.add<FifoQueue>("/A", "");

    t.enqueue("/A", {100});

    t.process(start + std::chrono::seconds(0));
    t.consumed("A", 100); // consume all tokens, but it is still active (not negative)

    t.process(start + std::chrono::seconds(5));
    t.consumed("A", 0); // There was nothing to consume

    t.enqueue("/A", {10, 10, 10, 10, 10, 10, 10, 10, 10, 10});
    t.process(start + std::chrono::seconds(5));
    t.consumed("A", 60); // 5 sec * 10 tokens/sec = 50 tokens + 1 extra request to go below zero

    t.process(start + std::chrono::seconds(100));
    t.consumed("A", 40); // Consume rest

    t.process(start + std::chrono::seconds(200));

    t.enqueue("/A", {95, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    t.process(start + std::chrono::seconds(200));
    t.consumed("A", 101); // check we cannot consume more than max_burst + 1 request

    t.process(start + std::chrono::seconds(100500));
    t.consumed("A", 3);
}

TEST(SchedulerThrottlerConstraint, PeekAndAvgLimits)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    // Burst = 100 token
    // Peek speed = 50 token/s for 10 seconds
    // Avg speed = 10 tokens/s afterwards
    t.add<ThrottlerConstraint>("/", "<max_burst>100.0</max_burst><max_speed>50.0</max_speed>");
    t.add<ThrottlerConstraint>("/avg", "<max_burst>5000.0</max_burst><max_speed>10.0</max_speed>");
    t.add<FifoQueue>("/avg/A", "");

    ResourceCost req_cost = 1;
    ResourceCost total_cost = 10000;
    for (int i = 0; i < total_cost / req_cost; i++)
        t.enqueue("/avg/A", {req_cost});

    double consumed = 0;
    for (int seconds = 0; seconds < 100; seconds++)
    {
        t.process(start + std::chrono::seconds(seconds));
        double arrival_curve = std::min(100.0 + 50.0 * seconds, 5000.0 + 10.0 * seconds) + req_cost;
        t.consumed("A", static_cast<ResourceCost>(arrival_curve - consumed));
        consumed = arrival_curve;
    }
}

TEST(SchedulerThrottlerConstraint, ThrottlerAndFairness)
{
    ResourceTest t;
    EventQueue::TimePoint start = std::chrono::system_clock::now();
    t.process(start, 0);

    t.add<ThrottlerConstraint>("/", "<max_burst>100.0</max_burst><max_speed>10.0</max_speed>");
    t.add<FairPolicy>("/fair", "");
    t.add<FifoQueue>("/fair/A", "<weight>10</weight>");
    t.add<FifoQueue>("/fair/B", "<weight>90</weight>");

    ResourceCost req_cost = 1;
    ResourceCost total_cost = 2000;
    for (int i = 0; i < total_cost / req_cost; i++)
    {
        t.enqueue("/fair/A", {req_cost});
        t.enqueue("/fair/B", {req_cost});
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
