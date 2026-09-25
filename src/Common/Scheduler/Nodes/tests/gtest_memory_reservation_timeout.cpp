#include <gtest/gtest.h>

#include <Common/MemorySpillScheduler.h>
#include <Common/MemoryTracker.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Processors/IProcessor.h>

#include <chrono>
#include <future>

using namespace DB;

namespace
{

struct ReservationTimeoutSchedulerTest : public ResourceTestBase
{
    SpaceSharedScheduler scheduler;

    ReservationTimeoutSchedulerTest()
    {
        scheduler.start(ThreadName::TEST_SCHEDULER);
    }

    ~ReservationTimeoutSchedulerTest()
    {
        scheduler.stop(true);
    }
};

struct ReservationTimeoutResource
{
    ReservationTimeoutSchedulerTest & test;
    SchedulerNodePtr root;

    explicit ReservationTimeoutResource(ReservationTimeoutSchedulerTest & test_)
        : test(test_)
    {
    }

    ~ReservationTimeoutResource()
    {
        if (!root)
            return;

        std::promise<void> removed;
        auto removed_future = removed.get_future();
        test.scheduler.event_queue.enqueue([this, &removed]
        {
            test.scheduler.removeChild(root.get());
            root.reset();
            removed.set_value();
        });
        removed_future.get();
    }

    AllocationQueue * initialize(ResourceCost limit)
    {
        root = std::make_shared<AllocationLimit>(test.scheduler.event_queue, SchedulerNodeInfo{}, limit);
        auto queue = std::make_shared<AllocationQueue>(test.scheduler.event_queue, SchedulerNodeInfo{});
        auto * queue_ptr = queue.get();
        queue->basename = "queue";
        root->attachChild(queue);

        std::promise<void> attached;
        auto attached_future = attached.get_future();
        test.scheduler.event_queue.enqueue([this, &attached]
        {
            test.scheduler.attachChild(root);
            attached.set_value();
        });
        attached_future.get();
        return queue_ptr;
    }
};

class BlockingReservationSpillProcessor final : public IProcessor
{
public:
    BlockingReservationSpillProcessor(std::promise<void> & started_, std::shared_future<void> release_, std::promise<void> & finished_)
        : started(started_)
        , release(std::move(release_))
        , finished(finished_)
    {
        spillable = true;
    }

    String getName() const override { return "BlockingReservationSpillProcessor"; }

    ProcessorMemoryStats getMemoryStats() override
    {
        return {.spillable_memory_bytes = 4096, .need_reserved_memory_bytes = 0};
    }

    bool spillForMemoryReservation() override
    {
        started.set_value();
        release.wait();
        finished.set_value();
        return false;
    }

private:
    std::promise<void> & started;
    std::shared_future<void> release;
    std::promise<void> & finished;
};

TEST(SchedulerSpaceShared, SingleReservationWorkerHonorsForcedSpillTimeout)
{
    ReservationTimeoutSchedulerTest test;
    ReservationTimeoutResource resource(test);
    AllocationQueue * queue = resource.initialize(10000);

    ResourceLink link;
    link.allocation_queue = queue;

    MemoryTracker tracker;
    auto spill_scheduler = std::make_shared<MemorySpillScheduler>(false);

    std::promise<void> spill_started;
    auto spill_started_future = spill_started.get_future();
    std::promise<void> release_spill;
    auto release_spill_future = release_spill.get_future().share();
    std::promise<void> spill_finished;
    auto spill_finished_future = spill_finished.get_future();
    auto processor = std::make_shared<BlockingReservationSpillProcessor>(
        spill_started, release_spill_future, spill_finished);
    spill_scheduler->registerProcessor(processor);

    MemoryReservation::Settings settings;
    settings.force_spill_before_eviction = true;
    settings.recovery_timeout_ms = 100;
    settings.pressure_policy.max_allocation_before_suction_bytes = 1;

    MemoryReservation reservation(link, "requester", 0, settings);
    reservation.setMemorySpillScheduler(spill_scheduler);

    tracker.adjustWithUntrackedMemory(8000);
    reservation.syncWithMemoryTracker(&tracker);

    tracker.adjustWithUntrackedMemory(3000);
    auto growth = std::async(std::launch::async, [&]
    {
        reservation.syncWithMemoryTracker(&tracker);
    });

    ASSERT_EQ(spill_started_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);

    /// There is deliberately no second reservation worker. The blocked spill callback must not
    /// keep the only caller inside forced spilling beyond the configured timeout.
    const auto growth_status = growth.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(growth_status, std::future_status::ready);

    /// Unblock the detached spill task before destroying the processor/scheduler, even when the
    /// timeout assertion above fails.
    release_spill.set_value();
    EXPECT_EQ(spill_finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);

    EXPECT_THROW(growth.get(), DB::Exception);
    tracker.adjustWithUntrackedMemory(-tracker.get());
}

}
