#include <chrono>
#include <gtest/gtest.h>

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
