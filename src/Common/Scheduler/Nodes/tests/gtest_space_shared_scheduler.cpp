#include <gtest/gtest.h>

#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/FairAllocation.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/MemoryTracker.h>

#include <barrier>
#include <cstdlib>
#include <future>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int MEMORY_RESERVATION_KILLED;
}

struct SpaceSharedTest : public ResourceTestBase
{
    SpaceSharedScheduler scheduler;

    SpaceSharedTest()
    {
        scheduler.start(ThreadName::TEST_SCHEDULER);
    }

    ~SpaceSharedTest()
    {
        scheduler.stop(true);
    }
};

struct SpaceSharedResourceHolder
{
    SpaceSharedTest & t;
    SchedulerNodePtr root_node;

    explicit SpaceSharedResourceHolder(SpaceSharedTest & t_)
        : t(t_)
    {}

    ~SpaceSharedResourceHolder()
    {
        unregisterResource();
    }

    AllocationLimit * addLimit(const String & path, ResourceCost max_allocated)
    {
        auto node = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, max_allocated);
        if (path == "/")
        {
            root_node = node;
            return node.get();
        }
        node->basename = path.substr(path.rfind('/') + 1);
        root_node->attachChild(node);
        return node.get();
    }

    AllocationQueue * addQueue(const String & path, Int64 max_queued = std::numeric_limits<Int64>::max())
    {
        auto node = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{}, max_queued);
        node->basename = path.substr(path.rfind('/') + 1);
        root_node->attachChild(node);
        return node.get();
    }

    /// Attaches a `FairAllocation` under `parent` (which must already be part of the detached subtree).
    FairAllocation * addFair(const String & path, ISpaceSharedNode * parent, double weight = 1.0)
    {
        auto node = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo(weight));
        node->basename = path.substr(path.rfind('/') + 1);
        parent->attachChild(node);
        return node.get();
    }

    /// Attaches an `AllocationQueue` under an explicit `parent` with a given fairness `weight`.
    AllocationQueue * addQueueUnder(const String & path, ISpaceSharedNode * parent, double weight = 1.0,
        Int64 max_queued = std::numeric_limits<Int64>::max())
    {
        auto node = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo(weight), max_queued);
        node->basename = path.substr(path.rfind('/') + 1);
        parent->attachChild(node);
        return node.get();
    }

    /// Sets the soft limit on the scheduler thread and waits for it to take effect.
    void setSoftLimit(AllocationLimit * limit, ResourceCost soft_limit)
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([&] { limit->updateSoftLimit(soft_limit); p.set_value(); });
        f.get();
    }

    /// Blocks until the scheduler thread has drained all events enqueued so far (FIFO barrier).
    void sync()
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([&] { p.set_value(); });
        f.get();
    }

    void registerResource()
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([this, &p]
        {
            t.scheduler.attachChild(root_node);
            p.set_value();
        });
        f.get();
    }

    void unregisterResource()
    {
        std::promise<void> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([this, &p]
        {
            t.scheduler.removeChild(root_node.get());
            root_node.reset(); // Destroy subtree on scheduler thread to satisfy chassert in ~ISchedulerNode
            p.set_value();
        });
        f.get();
    }
};


/// A mock allocation for exercising the reclaimable/spill machinery deterministically. Unlike
/// `MemoryReservation` (whose `spillAllocation` is a no-op), it records spill signals and lets the test
/// simulate a query reacting to one (report a lower reclaimable total, then decrease). Lock ordering
/// mirrors `MemoryReservation`: AllocationQueue::mutex -> SpillableAllocation::mutex, so queue operations
/// are always invoked without `mutex` held.
struct SpillableAllocation : public ResourceAllocation
{
    SpillableAllocation(AllocationQueue * queue_, const String & name_, ResourceCost initial_size)
        : ResourceAllocation(*queue_, name_)
    {
        real_size = initial_size;
        if (initial_size > 0)
            increase_enqueued = true;
        queue.insertAllocation(*this, initial_size); // scheduler thread may call back after this
        if (initial_size > 0) // Block until admitted, like MemoryReservation with reserve_memory > 0
        {
            std::unique_lock lock(mutex);
            cv.wait(lock, [this] { return !increase_enqueued || fail_reason; });
            if (fail_reason)
                std::rethrow_exception(fail_reason);
        }
    }

    ~SpillableAllocation() override
    {
        {
            std::unique_lock lock(mutex);
            if (removed || fail_reason)
                return;
            real_size = 0;
        }
        queue.removeAllocation(*this);
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return removed || fail_reason; });
    }

    /// Grows/shrinks the allocation to `new_size` and waits until the scheduler has applied it.
    void setSize(ResourceCost new_size)
    {
        ResourceCost inc = 0;
        ResourceCost dec = 0;
        {
            std::unique_lock lock(mutex);
            real_size = new_size;
            if (new_size > allocated_size)
            {
                inc = new_size - allocated_size;
                increase_enqueued = true;
            }
            else if (new_size < allocated_size)
            {
                dec = allocated_size - new_size;
                decrease_enqueued = true;
            }
        }
        if (inc > 0)
            queue.increaseAllocation(*this, inc);
        else if (dec > 0)
            queue.decreaseAllocation(*this, dec);
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return fail_reason || (!increase_enqueued && !decrease_enqueued); });
        if (fail_reason)
            std::rethrow_exception(fail_reason);
    }

    /// Reports the absolute reclaimable total to the scheduler (advisory, non-blocking).
    void reportReclaimable(ResourceCost total)
    {
        queue.setReclaimable(*this, total);
    }

    void waitSpills(size_t n)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return spills >= n; });
    }

    size_t spillCount()
    {
        std::unique_lock lock(mutex);
        return spills;
    }

    ResourceCost lastSpillAtLeast()
    {
        std::unique_lock lock(mutex);
        return last_spill_at_least;
    }

    ResourceCost size()
    {
        std::unique_lock lock(mutex);
        return allocated_size;
    }

private: // interaction with the scheduler thread
    void increaseApproved(const IncreaseRequest & increase) override
    {
        std::unique_lock lock(mutex);
        allocated_size += increase.size;
        increase_enqueued = false;
        cv.notify_all();
    }

    void decreaseApproved(const DecreaseRequest & decrease) override
    {
        std::unique_lock lock(mutex);
        allocated_size -= decrease.size;
        decrease_enqueued = false;
        if (decrease.removing_allocation)
            removed = true;
        cv.notify_all();
    }

    void allocationFailed(const std::exception_ptr & reason) override
    {
        std::unique_lock lock(mutex);
        fail_reason = reason;
        removed = true;
        allocated_size = 0;
        cv.notify_all();
    }

    void killAllocation(const std::exception_ptr & reason) override
    {
        std::unique_lock lock(mutex);
        kill_reason = reason;
        cv.notify_all();
    }

    void spillAllocation(ResourceCost at_least_bytes) override
    {
        std::unique_lock lock(mutex);
        ++spills;
        last_spill_at_least = at_least_bytes;
        cv.notify_all();
    }

    std::mutex mutex;
    std::condition_variable cv;
    std::exception_ptr kill_reason;
    std::exception_ptr fail_reason;
    bool increase_enqueued = false;
    bool decrease_enqueued = false;
    bool removed = false;
    size_t spills = 0;
    ResourceCost last_spill_at_least = 0;
    ResourceCost allocated_size = 0;
    ResourceCost real_size = 0;
};


TEST(SchedulerSpaceShared, Smoke)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000000); // 1GB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    // Create a reservation with initial size
    {
        MemoryReservation reservation(link, "test_reservation", 1000);
        // Reservation should be approved immediately since we're under the limit
        // Destructor will clean up
    }

    // Create multiple reservations
    {
        MemoryReservation res1(link, "res1", 1000);
        MemoryReservation res2(link, "res2", 2000);
        MemoryReservation res3(link, "res3", 3000);
        // All should be approved
    }
}


TEST(SchedulerSpaceShared, ReservationWithMemoryTracker)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000000); // 1GB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    MemoryTracker tracker;

    // Test 1: Increasing memory usage progressively
    // Note: We don't call syncWithMemoryTracker after decreasing the tracker because
    // decreases are async and would compete with the destructor's final decrease.
    {
        MemoryReservation reservation(link, "test_increasing", 1000);

        // Sync with memory tracker when tracker has 0 - uses reserved amount (1000)
        reservation.syncWithMemoryTracker(&tracker);

        // Simulate memory allocation that exceeds reserved amount
        tracker.adjustWithUntrackedMemory(5000); // Now tracker shows 5000
        EXPECT_EQ(tracker.get(), 5000);
        reservation.syncWithMemoryTracker(&tracker);

        // Simulate more memory allocation
        tracker.adjustWithUntrackedMemory(3000); // Now tracker shows 8000
        EXPECT_EQ(tracker.get(), 8000);
        reservation.syncWithMemoryTracker(&tracker);

        // Add even more to test progressive increase
        tracker.adjustWithUntrackedMemory(2000); // Now tracker shows 10000
        EXPECT_EQ(tracker.get(), 10000);
        reservation.syncWithMemoryTracker(&tracker);

        // Reset tracker before destruction - do NOT call sync after decreasing
        // The destructor handles the final decrease properly
        tracker.adjustWithUntrackedMemory(-10000);
        EXPECT_EQ(tracker.get(), 0);
    }

    // Test 2: Start above reserved and keep increasing
    {
        MemoryReservation reservation(link, "test_above_reserved", 2000);

        // Start with allocation higher than reserved
        tracker.adjustWithUntrackedMemory(4000);
        EXPECT_EQ(tracker.get(), 4000);
        reservation.syncWithMemoryTracker(&tracker);

        // Increase further
        tracker.adjustWithUntrackedMemory(1000); // Now 5000
        EXPECT_EQ(tracker.get(), 5000);
        reservation.syncWithMemoryTracker(&tracker);

        // Increase again
        tracker.adjustWithUntrackedMemory(500); // Now 5500
        EXPECT_EQ(tracker.get(), 5500);
        reservation.syncWithMemoryTracker(&tracker);

        // Reset tracker - destructor handles the decrease
        tracker.adjustWithUntrackedMemory(-5500);
        EXPECT_EQ(tracker.get(), 0);
    }

    // Test 3: Multiple syncs with same value (idempotent)
    {
        MemoryReservation reservation(link, "test_idempotent", 1000);

        tracker.adjustWithUntrackedMemory(3000);
        EXPECT_EQ(tracker.get(), 3000);

        // Multiple syncs with same value should be idempotent
        reservation.syncWithMemoryTracker(&tracker);
        reservation.syncWithMemoryTracker(&tracker);
        reservation.syncWithMemoryTracker(&tracker);

        // Increase and sync again
        tracker.adjustWithUntrackedMemory(1000); // Now 4000
        reservation.syncWithMemoryTracker(&tracker);
        reservation.syncWithMemoryTracker(&tracker);

        // Reset tracker - destructor handles the decrease
        tracker.adjustWithUntrackedMemory(-4000);
        EXPECT_EQ(tracker.get(), 0);
    }
}


TEST(SchedulerSpaceShared, LimitEnforcement)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000); // 10KB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    std::atomic<bool> res2_completed{false};

    // Barrier to ensure res1 is created before res2 tries to create its reservation
    std::barrier<> sync_barrier(2);

    // Second reservation will need to wait because first takes most of the limit
    std::thread t2([&]
    {
        // Wait for res1 to be created first
        sync_barrier.arrive_and_wait();
        MemoryReservation res2(link, "res2", 5000);
        res2_completed = true;
    });

    {
        // Create first reservation taking most of the limit - inside scope
        MemoryReservation res1(link, "res1", 8000);

        // Signal res1 is created, res2 can now try to create its reservation
        sync_barrier.arrive_and_wait();

        // res2 should be blocked since 8000 + 5000 > 10000
        // Give scheduler time to process res2's request and block it
        std::this_thread::yield();
        EXPECT_FALSE(res2_completed);

        // res1 will be destroyed here, allowing res2 to proceed
    }

    // Now res2 should complete
    t2.join();
    EXPECT_TRUE(res2_completed);
}


TEST(SchedulerSpaceShared, ConcurrentReservations)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000); // 1MB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    constexpr int num_threads = 10;
    constexpr int reservations_per_thread = 100;

    std::barrier<> start_barrier(num_threads + 1);
    std::atomic<int> completed{0};

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&, i]
        {
            start_barrier.arrive_and_wait();
            for (int j = 0; j < reservations_per_thread; ++j)
            {
                MemoryReservation res(link, fmt::format("res_{}_{}", i, j), 100);
                // Small delay to increase interleaving
                std::this_thread::yield();
            }
            completed++;
        });
    }

    // Start all threads simultaneously
    start_barrier.arrive_and_wait();

    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(completed.load(), num_threads);
}


TEST(SchedulerSpaceShared, KillDuringPendingIncrease)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 100000); // 100KB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    std::atomic<bool> exception_caught{false};

    // Barrier to sync: both reservations created
    std::barrier<> sync_barrier(2);

    // Victim thread - creates a reservation consuming >50% of the limit
    std::thread victim_thread([&]
    {
        MemoryTracker tracker;
        try
        {
            MemoryReservation res(link, "victim", 60000); // 60KB = 60% of limit

            // Signal that victim reservation is created
            sync_barrier.arrive_and_wait();

            // Keep calling syncWithMemoryTracker until we get killed
            // The killer will increase and evict us, causing an exception
            while (true)
            {
                res.syncWithMemoryTracker(&tracker);
                std::this_thread::yield();
            }
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::MEMORY_RESERVATION_KILLED);
            exception_caught = true;
        }
    });

    // Killer thread - creates a small reservation, then increases to trigger eviction
    std::thread killer_thread([&]
    {
        MemoryTracker tracker;
        try
        {
            // Start with a small reservation (30KB = 30% of limit)
            MemoryReservation killer(link, "killer", 30000);

            // Wait for victim to be ready
            sync_barrier.arrive_and_wait();

            // Increase tracker to 50KB, which will trigger an increase request
            // Total would be 60KB + 50KB = 110KB > 100KB limit, so victim gets evicted
            tracker.adjustWithUntrackedMemory(50000);
            killer.syncWithMemoryTracker(&tracker);

            // Reset tracker before destruction
            tracker.adjustWithUntrackedMemory(-50000);
        }
        catch (...) // Ok: not expected, but FAIL() handles it
        {
            // This should not happen - the killer is smaller than the victim,
            // so the eviction policy should kill the victim, not the killer
            FAIL() << "Killer should not be killed - it is smaller than the victim";
        }
    });

    victim_thread.join();
    killer_thread.join();

    // The victim must have been killed and caught an exception
    EXPECT_TRUE(exception_caught);

    // The key thing is that no assertion failure occurred during cleanup
    // (the original bug would cause allocated_size >= decrease.size assertion to fail)
}


/// Regression test for a deadlock where a never-admitted allocation that self-kills leaves
/// `AllocationLimit::allocation_to_kill` dangling, so the next over-limit allocation blocks forever
/// instead of being killed (observed as a 600s timeout in
/// `test_scheduler_memory::test_max_memory_limit`).
///
/// A reservation created with `reserved_size == 0` is never admitted: its first increase (driven by
/// the memory tracker) is the one that hits the limit, so it selects itself as the victim. It is then
/// removed via the local path in `AllocationQueue::processActivation`, which does NOT drive a
/// `removing_allocation` decrease up to `AllocationLimit::approveDecrease` — the only place (besides
/// subtree detach) that used to clear `allocation_to_kill`. The fix clears the pointer in
/// `setIncrease` once there is no increase request left to satisfy.
TEST(SchedulerSpaceShared, SelfKillDoesNotBlockNextAllocation)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000); // 10KB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    // Drives a never-admitted reservation over the limit; it must kill itself and throw.
    auto run_over_limit = [&](const String & id)
    {
        MemoryTracker tracker;
        tracker.adjustWithUntrackedMemory(20000); // 20KB > 10KB limit
        MemoryReservation res(link, id, 0); // reserved_size == 0 -> never admitted
        res.syncWithMemoryTracker(&tracker);
        tracker.adjustWithUntrackedMemory(-20000);
    };

    // First over-limit allocation self-kills. Before the fix this left `allocation_to_kill` dangling.
    EXPECT_THROW(run_over_limit("first"), DB::Exception);

    // The second over-limit allocation must also be killed, not block forever. Run it on a separate
    // thread guarded by a generous deadline so a regression surfaces as a clear failure instead of
    // hanging the whole test binary. The deadline is a deadlock detector only: with the fix the work
    // completes in microseconds, so even heavily-sanitized/slow builds never approach it.
    std::promise<void> done;
    auto done_future = done.get_future();
    std::thread second([&]
    {
        EXPECT_THROW(run_over_limit("second"), DB::Exception);
        done.set_value();
    });

    ASSERT_EQ(done_future.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "Second over-limit allocation blocked forever: `allocation_to_kill` was not cleared "
           "after the first allocation self-killed.";
    second.join();
}


/// Test that multiple syncs with memory tracker work correctly
TEST(SchedulerSpaceShared, MultipleMemoryTrackerSyncs)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000); // 1MB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    MemoryTracker tracker;

    {
        MemoryReservation res(link, "test", 5000); // Reserve 5KB minimum

        // Multiple syncs
        for (int i = 0; i < 10; ++i)
        {
            res.syncWithMemoryTracker(&tracker);
        }
    }
}


/// Test that the reported reclaimable amount is aggregated and propagated up the tree, kept clamped to
/// the allocation size, and removed when the allocation leaves.
TEST(SchedulerSpaceShared, ReclaimablePropagation)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 1000000); // 1MB limit (root)
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    // Flush the scheduler thread (so any pending activation is processed first) and read a node's
    // `reclaimable` aggregate on the scheduler thread, where it may be safely accessed.
    auto reclaimable_of = [&](ISpaceSharedNode * node) -> ResourceCost
    {
        std::promise<ResourceCost> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([&] { p.set_value(node->reclaimable); });
        return f.get();
    };

    {
        MemoryReservation res(link, "res", 1000); // admitted -> allocated == 1000

        // Nothing reclaimable initially.
        EXPECT_EQ(reclaimable_of(queue), 0);
        EXPECT_EQ(reclaimable_of(limit), 0);
        EXPECT_EQ(reclaimable_of(&t.scheduler), 0);

        // Report 600 reclaimable; it propagates to the queue, the limit and the root.
        queue->setReclaimable(res, 600);
        EXPECT_EQ(reclaimable_of(queue), 600);
        EXPECT_EQ(reclaimable_of(limit), 600);
        EXPECT_EQ(reclaimable_of(&t.scheduler), 600);

        // Lowering the report (delta -300) propagates too.
        queue->setReclaimable(res, 300);
        EXPECT_EQ(reclaimable_of(queue), 300);
        EXPECT_EQ(reclaimable_of(limit), 300);
        EXPECT_EQ(reclaimable_of(&t.scheduler), 300);

        // Reporting more than allocated is clamped to the allocation size (1000).
        queue->setReclaimable(res, 5000);
        EXPECT_EQ(reclaimable_of(queue), 1000);
        EXPECT_EQ(reclaimable_of(limit), 1000);
        EXPECT_EQ(reclaimable_of(&t.scheduler), 1000);

        // res is destroyed here -> its reclaimable must be removed from the aggregate.
    }

    EXPECT_EQ(reclaimable_of(queue), 0);
    EXPECT_EQ(reclaimable_of(limit), 0);
    EXPECT_EQ(reclaimable_of(&t.scheduler), 0);
}


/// Test rapid creation and destruction of reservations
TEST(SchedulerSpaceShared, RapidCreateDestroy)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000); // 1MB limit
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    for (int i = 0; i < 1000; ++i)
    {
        MemoryReservation res(link, fmt::format("rapid_{}", i), 100);
        // Immediate destruction
    }
}


/// A query over the soft limit but reporting nothing reclaimable is never asked to spill (fail-close,
/// invariant I6). Once it reports reclaimable memory, the scheduler asks it to reclaim `allocated - soft`.
TEST(SchedulerSpaceShared, SoftLimitFailCloseThenSpill)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 5000);

    SpillableAllocation a(queue, "a", 8000); // admitted; 8000 > soft 5000 but nothing reclaimable yet
    EXPECT_EQ(a.spillCount(), 0);

    a.reportReclaimable(6000);
    a.waitSpills(1);
    EXPECT_EQ(a.spillCount(), 1);
    EXPECT_EQ(a.lastSpillAtLeast(), 3000); // need = allocated(8000) - soft(5000)
}


/// Among several reclaimable allocations in one queue, the largest is asked to spill first (Progress /
/// invariant I8), and only one spill is in flight at a time (decision D2).
TEST(SchedulerSpaceShared, SoftLimitSpillsLargestInQueue)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 10000);

    SpillableAllocation a(queue, "a", 3000);
    SpillableAllocation b(queue, "b", 6000);
    // Both report reclaimable while still under the soft limit (9000 <= 10000) — no spill yet.
    a.reportReclaimable(3000);
    b.reportReclaimable(6000);
    EXPECT_EQ(a.spillCount(), 0);
    EXPECT_EQ(b.spillCount(), 0);

    // Cross the soft limit: allocated becomes 5000 + 6000 = 11000. `b` (6000) is the largest reclaimable.
    a.setSize(5000);
    b.waitSpills(1);
    EXPECT_EQ(b.spillCount(), 1);
    EXPECT_EQ(b.lastSpillAtLeast(), 1000); // need = 11000 - 10000
    EXPECT_EQ(a.spillCount(), 0); // one spill at a time: the smaller reclaimable is not signaled
}


/// Spill selection descends the tree skipping subtrees with nothing reclaimable: a larger but
/// unreclaimable allocation is passed over in favor of a smaller reclaimable one in a sibling workload.
TEST(SchedulerSpaceShared, SoftLimitDescendsFairSkippingUnreclaimable)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    FairAllocation * fair = r.addFair("/fair", limit);
    AllocationQueue * q1 = r.addQueueUnder("/fair/q1", fair);
    AllocationQueue * q2 = r.addQueueUnder("/fair/q2", fair);
    r.registerResource();
    r.setSoftLimit(limit, 10000);

    SpillableAllocation a1(q1, "a1", 8000); // larger, but never reports anything reclaimable
    SpillableAllocation a2(q2, "a2", 3000); // smaller, reclaimable
    a2.reportReclaimable(3000);

    a2.waitSpills(1);
    EXPECT_EQ(a2.spillCount(), 1);
    EXPECT_EQ(a2.lastSpillAtLeast(), 1000); // need = 11000 - 10000
    EXPECT_EQ(a1.spillCount(), 0); // unreclaimable subtree skipped
}


/// After a victim makes progress (a decrease), the one-at-a-time gate reopens and the scheduler re-signals
/// while still over the soft limit; once back under it, no further spill is requested.
TEST(SchedulerSpaceShared, SpillReSignalsUntilUnderSoftLimit)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 5000);

    SpillableAllocation a(queue, "a", 8000);
    a.reportReclaimable(8000);
    a.waitSpills(1);
    EXPECT_EQ(a.lastSpillAtLeast(), 3000);

    // Simulate a partial reaction: report less reclaimable and shrink, but stay above the soft limit.
    a.reportReclaimable(4000);
    a.setSize(6000); // decrease by 2000 -> allocated 6000 > soft 5000, still reclaimable
    a.waitSpills(2);
    EXPECT_EQ(a.lastSpillAtLeast(), 1000); // need = 6000 - 5000

    // Now drop below the soft limit: the episode ends, no more spills are requested.
    a.reportReclaimable(1000);
    a.setSize(4000); // allocated 4000 <= soft 5000
    EXPECT_EQ(a.spillCount(), 2);
}


/// Reads `SCHED_PERF_ITERS` (default 1000: functional coverage). Set it large for a manual perf run,
/// ideally against a non-sanitizer release build (`build_release/src/unit_tests_dbms
/// --gtest_filter='*ReclaimablePerf*'`).
static size_t perfIters()
{
    if (const char * env = std::getenv("SCHED_PERF_ITERS")) // NOLINT(concurrency-mt-unsafe): test setup, single-threaded
    {
        if (Int64 v = strtoll(env, nullptr, 10); v > 0)
            return static_cast<size_t>(v);
    }
    return 1000;
}

static void reportPerf(const String & name, size_t ops, double seconds)
{
    double ops_per_sec = seconds > 0 ? static_cast<double>(ops) / seconds : 0.0;
    double ns_per_op = ops > 0 ? seconds * 1e9 / static_cast<double>(ops) : 0.0;
    fmt::print(stderr, "[ perf     ] {:<34} {:>9.1f} ns/op   {:>14.0f} ops/sec\n", name, ns_per_op, ops_per_sec);
}

/// Builds `limit -> fair^depth -> queue`; returns {limit, leaf queue}. `depth` internal fair nodes let us
/// measure how scheduler cost scales with hierarchy depth (number of internal nodes on the root-leaf path).
static std::pair<AllocationLimit *, AllocationQueue *> buildChain(SpaceSharedResourceHolder & r, size_t depth)
{
    AllocationLimit * limit = r.addLimit("/", 1000000000);
    ISpaceSharedNode * parent = limit;
    String path;
    for (size_t i = 0; i < depth; ++i)
    {
        path += fmt::format("/f{}", i);
        parent = r.addFair(path, parent);
    }
    AllocationQueue * queue = r.addQueueUnder(path + "/q", parent);
    return {limit, queue};
}

/// Isolates the pure scheduler-thread cost of `selectAllocationToSpill` (the spill victim descent): runs
/// the loop entirely on the scheduler thread, so there is no cross-thread round-trip or event-queue cost.
static double measureDescent(SpaceSharedTest & t, AllocationLimit * limit, size_t n)
{
    std::promise<double> pr;
    auto fut = pr.get_future();
    t.scheduler.event_queue.enqueue([&, limit, n]
    {
        String details;
        ResourceAllocation * sink = nullptr;
        Stopwatch sw;
        for (size_t i = 0; i < n; ++i)
            sink = limit->selectAllocationToSpill(1, details);
        double seconds = sw.elapsedSeconds();
        ASSERT_NE(sink, nullptr); // the tree is populated; keeps the loop from being optimized away
        pr.set_value(seconds);
    });
    return fut.get();
}

/// Spill victim selection (`selectAllocationToSpill`) cost as a function of hierarchy DEPTH.
/// Expected O(depth): one reclaimable allocation, so each internal node just takes its set extremum.
TEST(SchedulerSpaceShared, ReclaimablePerfDescentByDepth)
{
    const size_t iters = perfIters();
    for (size_t depth : {0uz, 2uz, 8uz, 32uz})
    {
        SpaceSharedTest t;
        SpaceSharedResourceHolder r(t);
        auto [limit, queue] = buildChain(r, depth);
        r.registerResource();

        std::list<SpillableAllocation> allocs;
        allocs.emplace_back(queue, "a", 10000);
        allocs.back().reportReclaimable(5000);
        r.sync(); // ensure the reclaimable set is populated before measuring

        double seconds = measureDescent(t, limit, iters);
        reportPerf(fmt::format("descent depth={}", depth), iters, seconds);
    }
}

/// Spill victim selection cost as a function of WIDTH (sibling queues under one fair node), each holding a
/// reclaimable allocation. The reclaimable-filtered set is ordered, so taking its extremum stays about
/// constant regardless of width — this test demonstrates that spill selection does not degrade with the
/// number of sibling workloads.
TEST(SchedulerSpaceShared, ReclaimablePerfDescentByWidth)
{
    const size_t iters = perfIters();
    for (size_t width : {1uz, 16uz, 256uz})
    {
        SpaceSharedTest t;
        SpaceSharedResourceHolder r(t);
        AllocationLimit * limit = r.addLimit("/", 1000000000);
        FairAllocation * fair = r.addFair("/fair", limit);
        std::vector<AllocationQueue *> queues;
        for (size_t i = 0; i < width; ++i)
            queues.push_back(r.addQueueUnder(fmt::format("/fair/q{}", i), fair));
        r.registerResource();

        std::list<SpillableAllocation> allocs;
        for (size_t i = 0; i < width; ++i)
        {
            allocs.emplace_back(queues[i], fmt::format("a{}", i), 10000);
            allocs.back().reportReclaimable(5000);
        }
        r.sync();

        double seconds = measureDescent(t, limit, iters);
        reportPerf(fmt::format("descent width={}", width), iters, seconds);
    }
}

/// Spill victim selection cost as a function of the number of reclaimable ALLOCATIONS in a single queue.
/// The queue keeps them in an ordered set, so selecting the largest stays about constant regardless of N.
TEST(SchedulerSpaceShared, ReclaimablePerfDescentByAllocations)
{
    const size_t iters = perfIters();
    for (size_t count : {1uz, 100uz, 1000uz})
    {
        SpaceSharedTest t;
        SpaceSharedResourceHolder r(t);
        auto [limit, queue] = buildChain(r, 0);
        r.registerResource();

        std::list<SpillableAllocation> allocs;
        for (size_t i = 0; i < count; ++i)
        {
            allocs.emplace_back(queue, fmt::format("a{}", i), 10000);
            allocs.back().reportReclaimable(5000);
        }
        r.sync();

        double seconds = measureDescent(t, limit, iters);
        reportPerf(fmt::format("descent allocs={}", count), iters, seconds);
    }
}

/// End-to-end synchronous increase/decrease churn on one reclaimable allocation, swept over hierarchy
/// depth. Each `setSize` is a full round-trip through the scheduler thread (request, approve, propagate,
/// re-key the reclaimable set at every level), so this reflects realistic throughput including cross-thread
/// wakeup latency, not just the algorithmic cost.
TEST(SchedulerSpaceShared, ReclaimablePerfChurnByDepth)
{
    const size_t iters = perfIters();
    for (size_t depth : {0uz, 2uz, 8uz})
    {
        SpaceSharedTest t;
        SpaceSharedResourceHolder r(t);
        auto [limit, queue] = buildChain(r, depth);
        r.registerResource();
        (void)limit;

        SpillableAllocation a(queue, "a", 10000);
        a.reportReclaimable(5000); // reclaimable, so each resize also re-keys the reclaimable set
        r.sync();

        Stopwatch sw;
        for (size_t i = 0; i < iters; ++i)
        {
            a.setSize(20000);
            a.setSize(10000);
        }
        reportPerf(fmt::format("churn depth={}", depth), 2 * iters, sw.elapsedSeconds());
    }
}
