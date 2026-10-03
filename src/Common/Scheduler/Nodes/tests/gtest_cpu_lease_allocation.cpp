#include <gtest/gtest.h>

#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/Nodes/TimeShared/FifoQueue.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/TimeSharedScheduler.h>
#include <Common/setThreadName.h>

#include <chrono>
#include <future>
#include <thread>

using namespace DB;

namespace
{

void burnCPU(ResourceCost cpu_ns)
{
    UInt64 start = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    while (clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) - start < static_cast<UInt64>(cpu_ns))
    {
    }
}

}

/// Only worker threads compete for slots (like `CREATE RESOURCE cpu (WORKER THREAD)`), so the master request
/// is free and finishing it returns no slot. A single report that covers several quanta finishes only the master
/// request, and the master thread is preempted while its worker request is fully consumed and holds the only slot.
/// The next worker request needs that slot, so the query must give it back on preemption, or it waits for itself.
TEST(SchedulerCPULeaseAllocation, FullPreemptionReturnsConsumedSlots)
{
    TimeSharedScheduler scheduler;
    auto semaphore = std::make_shared<SemaphoreConstraint>(scheduler.event_queue, SchedulerNodeInfo{}, /*max_requests=*/ 1);
    auto queue = std::make_shared<FifoQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    queue->basename = "queue";
    semaphore->attachChild(queue);
    scheduler.attachChild(semaphore);
    scheduler.start(ThreadName::TEST_SCHEDULER);

    constexpr ResourceCost quantum_ns = 10'000'000;
    auto allocation = std::make_shared<CPULeaseAllocation>(
        /*max_threads=*/ 3,
        ResourceLink{},
        ResourceLink{.queue = queue.get()},
        CPULeaseSettings{.quantum_ns = quantum_ns, .report_ns = quantum_ns / 10});

    // The free master request is granted immediately, the first worker request takes the only slot,
    // and the second one waits in the queue.
    while (semaphore->getInflights().first != 1 || queue->getQueueLengthAndCost().first != 1)
        std::this_thread::yield();

    auto master = std::async(std::launch::async, [&]
    {
        auto slot = allocation->acquire();
        auto * lease = dynamic_cast<ISlotLease *>(slot.get());
        lease->startConsumption();
        burnCPU(4 * quantum_ns); // One processor step that consumes more than all three requests
        return lease->renew();
    });

    bool resumed = master.wait_for(std::chrono::seconds(30)) == std::future_status::ready;
    if (!resumed)
        allocation->free(); // Wakes the master thread, so the test fails instead of hanging
    EXPECT_TRUE(resumed) << "the preempted master thread waits for a slot that its own query holds";
    EXPECT_EQ(master.get(), resumed);

    allocation->free();
    scheduler.stop();
}
