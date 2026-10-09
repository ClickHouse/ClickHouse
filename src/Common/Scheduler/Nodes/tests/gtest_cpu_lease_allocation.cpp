#include <gtest/gtest.h>

#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/Nodes/TimeShared/RequestQueue.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/TimeSharedScheduler.h>
#include <Common/setThreadName.h>

#include <chrono>
#include <future>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

namespace
{

void burnCPU(ResourceCost cpu_ns)
{
    UInt64 start = clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID);
    while (clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) - start < static_cast<UInt64>(cpu_ns))
    {
    }
}

/// Checks that the scheduler thread still processes events
bool schedulerIsAlive(TimeSharedScheduler & scheduler)
{
    auto processed = std::make_shared<std::promise<void>>();
    auto future = processed->get_future();
    scheduler.event_queue.enqueue([processed] { processed->set_value(); });
    return future.wait_for(std::chrono::seconds(30)) == std::future_status::ready;
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
    auto queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    queue->basename = "queue";
    semaphore->attachChild(queue);
    scheduler.attachChild(semaphore);
    scheduler.start(ThreadName::TEST_SCHEDULER);

    constexpr ResourceCost quantum_ns = 10'000'000;
    CPULeaseSettings settings;
    settings.quantum_ns = quantum_ns;
    settings.report_ns = quantum_ns / 10;
    ResourceLink worker_link;
    worker_link.queue = queue.get();
    auto allocation = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 3, ResourceLink{}, worker_link, settings);

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

/// Master and worker slots are requested from different queues, and the worker queue is purged (as if its workload got a child).
/// Granting the master slot in the scheduler thread fails to enqueue the next (worker) request.
/// The allocation must fail and the scheduler thread must keep running.
TEST(SchedulerCPULeaseAllocation, GrantFailsAllocationIfNextQueueIsPurged)
{
    TimeSharedScheduler scheduler;
    auto master_queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    master_queue->basename = "master";
    scheduler.attachChild(master_queue);
    RequestQueue worker_queue(scheduler.event_queue, SchedulerNodeInfo{});
    worker_queue.purgeQueue();
    scheduler.start(ThreadName::TEST_SCHEDULER);

    ResourceLink master_link;
    master_link.queue = master_queue.get();
    ResourceLink worker_link;
    worker_link.queue = &worker_queue;
    auto allocation = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 2, master_link, worker_link, CPULeaseSettings{});

    bool failed = false;
    for (auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30); !failed && std::chrono::steady_clock::now() < deadline;)
    {
        try
        {
            EXPECT_EQ(allocation->tryAcquire(), nullptr);
            std::this_thread::yield();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::RESOURCE_ACCESS_DENIED);
            failed = true;
        }
    }
    EXPECT_TRUE(failed);
    EXPECT_TRUE(schedulerIsAlive(scheduler));

    allocation->free();
    scheduler.stop();
}

/// Same as above for `CPUSlotsAllocation`
TEST(SchedulerCPUSlotsAllocation, GrantFailsAllocationIfNextQueueIsPurged)
{
    TimeSharedScheduler scheduler;
    auto master_queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    master_queue->basename = "master";
    scheduler.attachChild(master_queue);
    RequestQueue worker_queue(scheduler.event_queue, SchedulerNodeInfo{});
    worker_queue.purgeQueue();
    scheduler.start(ThreadName::TEST_SCHEDULER);

    ResourceLink master_link;
    master_link.queue = master_queue.get();
    ResourceLink worker_link;
    worker_link.queue = &worker_queue;
    auto allocation = std::make_shared<CPUSlotsAllocation>(/*master_slots=*/ 1, /*worker_slots=*/ 1, master_link, worker_link);

    // The granted master slot may be acquired before the allocation fails
    std::vector<AcquiredSlotPtr> slots;
    auto acquire = std::async(std::launch::async, [&]
    {
        try
        {
            while (true)
                slots.push_back(allocation->acquire());
        }
        catch (const Exception & e)
        {
            return e.code();
        }
    });
    ASSERT_EQ(acquire.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    EXPECT_EQ(acquire.get(), ErrorCodes::RESOURCE_ACCESS_DENIED);
    EXPECT_LE(slots.size(), 1u);
    EXPECT_TRUE(schedulerIsAlive(scheduler));

    slots.clear();
    allocation.reset();
    scheduler.stop();
}
