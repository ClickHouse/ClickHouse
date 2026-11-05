#include <gtest/gtest.h>

#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/Nodes/TimeShared/RequestQueue.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/TimeSharedScheduler.h>
#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>

#include <chrono>
#include <future>
#include <thread>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlPreempted;
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

/// Waits until `predicate` holds, or gives up after a while, so a broken invariant fails the test instead of hanging it
template <typename Predicate>
bool waitUntil(Predicate predicate)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
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

/// Master and worker threads share one resource, which is the mode with parking. The worker parks and gives its
/// slot back, another query takes it, and the master runs out of its quantum and is preempted. The next grant
/// brings the query back to one allocated slot, held by the parked worker but unused. It must resume the master.
TEST(SchedulerCPULeaseAllocation, GrantResumesPreemptedThreadWhileOtherIsParked)
{
    TimeSharedScheduler scheduler;
    auto semaphore = std::make_shared<SemaphoreConstraint>(scheduler.event_queue, SchedulerNodeInfo{}, /*max_requests=*/ 2);
    auto queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    queue->basename = "queue";
    semaphore->attachChild(queue);
    scheduler.attachChild(semaphore);
    scheduler.start(ThreadName::TEST_SCHEDULER);

    constexpr ResourceCost quantum_ns = 10'000'000;
    CPULeaseSettings settings;
    settings.quantum_ns = quantum_ns;
    settings.report_ns = quantum_ns / 10;
    settings.parking_enabled = true;
    ResourceLink link;
    link.queue = queue.get();
    auto allocation = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 2, link, link, settings);

    // Both requests of the query are granted
    while (semaphore->getInflights().first != 2)
        std::this_thread::yield();
    auto master_slot = allocation->acquire();
    auto worker_slot = allocation->acquire();
    auto * master = dynamic_cast<ISlotLease *>(master_slot.get());
    auto * worker = dynamic_cast<ISlotLease *>(worker_slot.get());
    ASSERT_EQ(master->slot_id, 0u);
    ASSERT_EQ(worker->slot_id, 1u);

    // The worker parks and gives one slot back
    ASSERT_TRUE(worker->park());
    while (semaphore->getInflights().first != 1)
        std::this_thread::yield();

    // Another query takes the free slot and waits for one more, so the next request of the first query waits behind it
    auto other = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 2, link, link, settings);
    while (semaphore->getInflights().first != 2 || queue->getQueueLengthAndCost().first != 1)
        std::this_thread::yield();

    const auto preempted_before = CurrentMetrics::get(CurrentMetrics::ConcurrencyControlPreempted);
    std::atomic_bool other_freed = false;
    auto master_thread = std::async(std::launch::async, [&]
    {
        master->startConsumption();
        while (!other_freed)
        {
            burnCPU(settings.report_ns);
            if (!master->renew()) // Blocks once the master is preempted
                return false;
        }
        return true;
    });

    // The master consumes its last quantum, waits for the next one and is preempted
    while (CurrentMetrics::get(CurrentMetrics::ConcurrencyControlPreempted) == preempted_before)
        std::this_thread::yield();

    // The other query frees the slots, so the waiting request is granted
    other_freed = true;
    other->free();

    bool resumed = master_thread.wait_for(std::chrono::seconds(30)) == std::future_status::ready;
    if (!resumed)
        allocation->free(); // Wakes the master thread, so the test fails instead of hanging
    EXPECT_TRUE(resumed) << "the preempted master thread is not resumed while the only allocated slot belongs to a parked thread";
    EXPECT_EQ(master_thread.get(), resumed);

    worker->unpark();
    master_slot.reset();
    worker_slot.reset();
    allocation->free();
    scheduler.stop();
}

/// Master and worker threads share one resource, which is the mode with parking. A parked thread uses no CPU, so
/// parking gives its slot back to the scheduler at once. No thread consumes CPU here, so nothing else frees the slot.
TEST(SchedulerCPULeaseAllocation, ParkGivesSlotBackToScheduler)
{
    TimeSharedScheduler scheduler;
    auto semaphore = std::make_shared<SemaphoreConstraint>(scheduler.event_queue, SchedulerNodeInfo{}, /*max_requests=*/ 2);
    auto queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    queue->basename = "queue";
    semaphore->attachChild(queue);
    scheduler.attachChild(semaphore);
    scheduler.start(ThreadName::TEST_SCHEDULER);

    CPULeaseSettings settings;
    settings.parking_enabled = true;
    ResourceLink link;
    link.queue = queue.get();
    auto allocation = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 2, link, link, settings);

    // Both requests of the query are granted
    while (semaphore->getInflights().first != 2)
        std::this_thread::yield();
    auto master_slot = allocation->acquire();
    auto worker_slot = allocation->acquire();
    auto * worker = dynamic_cast<ISlotLease *>(worker_slot.get());

    // Another query waits for a slot
    auto other = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 1, link, link, settings);
    while (queue->getQueueLengthAndCost().first != 1)
        std::this_thread::yield();

    ASSERT_TRUE(worker->park());
    EXPECT_TRUE(waitUntil([&] { return queue->getQueueLengthAndCost().first == 0 && semaphore->getInflights().first == 2; }))
        << "the slot of the parked thread is not given back to the scheduler";

    worker->unpark();
    master_slot.reset();
    worker_slot.reset();
    other->free();
    allocation->free();
    scheduler.stop();
}

/// The allocation is shut down while two of its threads are parked. Their unparks come after the shutdown:
/// they must not throw and must not ask the scheduler for slots again.
TEST(SchedulerCPULeaseAllocation, UnparkAfterShutdown)
{
    TimeSharedScheduler scheduler;
    auto semaphore = std::make_shared<SemaphoreConstraint>(scheduler.event_queue, SchedulerNodeInfo{}, /*max_requests=*/ 3);
    auto queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    queue->basename = "queue";
    semaphore->attachChild(queue);
    scheduler.attachChild(semaphore);
    scheduler.start(ThreadName::TEST_SCHEDULER);

    CPULeaseSettings settings;
    settings.parking_enabled = true;
    ResourceLink link;
    link.queue = queue.get();
    auto allocation = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 3, link, link, settings);

    // All three requests are granted
    while (semaphore->getInflights().first != 3)
        std::this_thread::yield();
    auto master_slot = allocation->acquire();
    auto worker1_slot = allocation->acquire();
    auto worker2_slot = allocation->acquire();
    auto * worker1 = dynamic_cast<ISlotLease *>(worker1_slot.get());
    auto * worker2 = dynamic_cast<ISlotLease *>(worker2_slot.get());

    // Both workers park, and each gives one slot back
    ASSERT_TRUE(worker1->park());
    ASSERT_TRUE(worker2->park());
    EXPECT_TRUE(waitUntil([&] { return semaphore->getInflights().first == 1; }));

    // Shutdown with both workers parked releases the last slot
    allocation->free();
    EXPECT_TRUE(waitUntil([&] { return semaphore->getInflights().first == 0; }));

    EXPECT_NO_THROW(worker1->unpark());
    EXPECT_NO_THROW(worker2->unpark());
    EXPECT_EQ(queue->getQueueLengthAndCost().first, 0u) << "unpark after shutdown enqueued a resource request";
    EXPECT_EQ(semaphore->getInflights().first, 0);

    master_slot.reset();
    worker1_slot.reset();
    worker2_slot.reset();
    scheduler.stop();
}

/// A worker thread that renews the lease inside a task (a nested pipeline) cannot stop, so after the preemption
/// timeout it must keep waiting for a grant instead of running without one. Another query holds every slot here.
TEST(SchedulerCPULeaseAllocation, WorkerPreemptedInsideTaskWaitsForGrant)
{
    TimeSharedScheduler scheduler;
    auto semaphore = std::make_shared<SemaphoreConstraint>(scheduler.event_queue, SchedulerNodeInfo{}, /*max_requests=*/ 2);
    auto queue = std::make_shared<RequestQueue>(scheduler.event_queue, SchedulerNodeInfo{});
    queue->basename = "queue";
    semaphore->attachChild(queue);
    scheduler.attachChild(semaphore);
    scheduler.start(ThreadName::TEST_SCHEDULER);

    CPULeaseSettings settings;
    settings.preemption_timeout = std::chrono::milliseconds(1);
    ResourceLink link;
    link.queue = queue.get();

    // Another query takes both slots, so the request of the query waits in the queue
    auto other = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 2, link, link, settings);
    while (semaphore->getInflights().first != 2)
        std::this_thread::yield();
    auto allocation = std::make_shared<CPULeaseAllocation>(/*max_threads=*/ 2, link, link, settings);
    while (queue->getQueueLengthAndCost().first != 1)
        std::this_thread::yield();

    // Both threads run before any slot is granted, so the worker is preempted at its first report
    auto master_slot = allocation->acquire();
    auto worker_slot = allocation->acquire();
    ASSERT_EQ(worker_slot->slot_id, 1u);
    const auto preempted_before = CurrentMetrics::get(CurrentMetrics::ConcurrencyControlPreempted);
    auto worker_thread = std::async(std::launch::async, [&, slot = std::move(worker_slot)]() mutable
    {
        auto * worker = dynamic_cast<ISlotLease *>(slot.get());
        worker->startConsumption();
        burnCPU(settings.report_ns);
        worker->renewInTask();
        slot.reset();
    });

    EXPECT_TRUE(waitUntil([&]
    {
        return CurrentMetrics::get(CurrentMetrics::ConcurrencyControlPreempted) > preempted_before
            || worker_thread.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }));
    EXPECT_EQ(worker_thread.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout)
        << "a worker preempted inside a task stopped waiting for a grant after the preemption timeout";

    // The other query frees its slots, and the grants resume the worker
    other->free();
    bool resumed = worker_thread.wait_for(std::chrono::seconds(30)) == std::future_status::ready;
    if (!resumed)
        allocation->free(); // Wakes the worker thread, so the test fails instead of hanging
    EXPECT_TRUE(resumed) << "the worker preempted inside a task is not resumed by a grant";

    worker_thread.get();
    master_slot.reset();
    allocation->free();
    scheduler.stop();
}
