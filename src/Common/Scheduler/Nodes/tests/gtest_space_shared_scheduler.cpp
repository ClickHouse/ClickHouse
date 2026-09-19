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

namespace CurrentMetrics
{
    extern const Metric MemoryReservationApproved;
    extern const Metric MemoryReservationDemand;
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

    /// Replies to a spill request: reports the remaining reclaimable total and reopens the spill gates.
    void finishSpill(ResourceCost total)
    {
        queue.finishSpill(*this, total);
    }

    /// Simulates a spill reaction without waiting for the decrease approval: issues the decrease for the
    /// freed memory and immediately finishes the spill, so both reach the scheduler in one activation.
    /// Exercises the deferred re-evaluation path (the reply arrives while the decrease is still pending).
    void spillAndFinish(ResourceCost spilled_bytes, ResourceCost reclaimable_total)
    {
        {
            std::unique_lock lock(mutex);
            real_size -= spilled_bytes;
            decrease_enqueued = true;
        }
        queue.decreaseAllocation(*this, spilled_bytes);
        queue.finishSpill(*this, reclaimable_total);
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


/// Regression test: an increase issued while a decrease is still in flight must be sized against
/// the post-decrease allocation. It used to be sized against the current allocation, so the
/// approved total ended short by exactly the in-flight decrease and the sync waited forever.
TEST(SchedulerSpaceShared, IncreaseWhileDecreaseIsInFlight)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    /// Reads the aggregate on the scheduler thread, where it may be safely accessed.
    auto allocated_of = [&](ISpaceSharedNode * node) -> ResourceCost
    {
        std::promise<ResourceCost> p;
        auto f = p.get_future();
        t.scheduler.event_queue.enqueue([&] { p.set_value(node->allocated); });
        return f.get();
    };

    MemoryTracker tracker;
    {
        MemoryReservation res(link, "res", 100);
        tracker.adjustWithUntrackedMemory(800);
        res.syncWithMemoryTracker(&tracker);
        EXPECT_EQ(allocated_of(queue), 800);

        // Hold the scheduler thread so that the following requests stay in flight.
        std::promise<void> unblock;
        t.scheduler.event_queue.enqueue([f = unblock.get_future().share()] { f.get(); });

        tracker.adjustWithUntrackedMemory(-500); // 300
        res.syncWithMemoryTracker(&tracker); // enqueues a decrease of 500 and returns without waiting

        ResourceCost demand = CurrentMetrics::get(CurrentMetrics::MemoryReservationDemand);
        tracker.adjustWithUntrackedMemory(700); // 1000
        std::thread grow([&] { res.syncWithMemoryTracker(&tracker); });
        // Make sure the increase is enqueued while the decrease is still in flight
        while (CurrentMetrics::get(CurrentMetrics::MemoryReservationDemand) == demand)
            std::this_thread::yield();

        unblock.set_value();
        grow.join(); // used to hang forever: 800 - 500 + (1000 - 800) < 1000

        // Both requests approved: the allocation converges to the actual usage
        while (allocated_of(queue) != 1000)
            std::this_thread::yield();

        tracker.adjustWithUntrackedMemory(-1000);
    }
    EXPECT_EQ(allocated_of(queue), 0);
}


/// The sync must not return while the usage is covered only by capacity that an in-flight decrease
/// is about to release: once the decrease is approved, that capacity may be granted to another
/// workload. It used to return without waiting whenever the usage was below the current allocation.
TEST(SchedulerSpaceShared, SyncWaitsForPostDecreaseCoverage)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 1000000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    MemoryTracker tracker;
    MemoryReservation res(link, "res", 100);
    tracker.adjustWithUntrackedMemory(800);
    res.syncWithMemoryTracker(&tracker); // allocated == 800

    // Hold the scheduler thread so that the following requests stay in flight.
    std::promise<void> unblock;
    t.scheduler.event_queue.enqueue([f = unblock.get_future().share()] { f.get(); });

    tracker.adjustWithUntrackedMemory(-500); // 300
    res.syncWithMemoryTracker(&tracker); // enqueues a decrease of 500 and returns without waiting

    ResourceCost demand = CurrentMetrics::get(CurrentMetrics::MemoryReservationDemand);
    ResourceCost approved = CurrentMetrics::get(CurrentMetrics::MemoryReservationApproved);
    tracker.adjustWithUntrackedMemory(300); // 600: covered by the current 800, but not by 800 - 500
    std::thread grow([&]
    {
        res.syncWithMemoryTracker(&tracker);
        // The compensating increase (+300) must be approved by the time the sync returns;
        // the decrease (-500) may or may not be. No change at all means the sync did not wait.
        ResourceCost diff = CurrentMetrics::get(CurrentMetrics::MemoryReservationApproved) - approved;
        EXPECT_TRUE(diff == 300 || diff == -200) << diff;
    });
    // Make sure the increase is enqueued while the decrease is still in flight
    while (CurrentMetrics::get(CurrentMetrics::MemoryReservationDemand) == demand)
        std::this_thread::yield();

    unblock.set_value();
    grow.join();

    tracker.adjustWithUntrackedMemory(-600);
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


/// A query over the soft limit but reporting nothing reclaimable is never asked to spill (fail-close).
/// Once it reports reclaimable memory, the scheduler asks it to reclaim `allocated - soft`.
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


/// Among several reclaimable allocations in one queue, the largest is asked to spill first (the same order
/// the kill path uses), and no second signal is issued while the first request is outstanding.
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


/// While over the soft limit, each acknowledgement — a decrease OR a drop in reported reclaimable — reopens
/// the one-at-a-time gate and the scheduler re-signals with the updated `need`; once back under the soft
/// limit the episode ends and no further spill is requested. The exact number of (coalescing) re-signals is
/// intentionally not asserted, only the requested amount and the terminal "no more spills" state.
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
    EXPECT_EQ(a.lastSpillAtLeast(), 3000); // need = allocated(8000) - soft(5000)

    // Partial reaction: shrink and reply, still above the soft limit. The reply reopens the gate and the
    // next signal carries the updated need for the smaller allocation.
    a.setSize(6000); // decrease by 2000 -> allocated 6000 > soft 5000, still reclaimable
    a.finishSpill(6000);
    a.waitSpills(2);
    EXPECT_EQ(a.lastSpillAtLeast(), 1000); // need = allocated(6000) - soft(5000)

    // Drop below the soft limit and reply: the episode ends. After the scheduler settles, no new signal.
    a.setSize(4000); // allocated 4000 <= soft 5000
    a.finishSpill(4000);
    r.sync();
    size_t settled = a.spillCount();
    r.sync();
    EXPECT_EQ(a.spillCount(), settled); // no further spill once under the soft limit
    EXPECT_EQ(settled, 2u);
}


/// A non-removing shrink clamps the allocation's reported reclaimable down to the new `allocated`
/// and keeps the reclaimable aggregate consistent up the whole tree. Without the clamp a
/// spill-signaled allocation that decreases before re-reporting would keep `reclaimable > allocated` and
/// could be re-picked as a spill victim for memory it has already released.
TEST(SchedulerSpaceShared, ReclaimableClampedOnShrink)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    // Read a node's `reclaimable` on the scheduler thread, where it may be safely accessed.
    auto reclaimable_of = [&](ISpaceSharedNode * node) -> ResourceCost
    {
        std::promise<ResourceCost> p;
        auto fut = p.get_future();
        t.scheduler.event_queue.enqueue([&] { p.set_value(node->reclaimable); });
        return fut.get();
    };

    SpillableAllocation a(queue, "a", 8000);
    a.reportReclaimable(8000);
    EXPECT_EQ(reclaimable_of(queue), 8000);
    EXPECT_EQ(reclaimable_of(limit), 8000);
    EXPECT_EQ(reclaimable_of(&t.scheduler), 8000);

    // Shrink to 3000 without re-reporting: the stale reclaimable (8000) exceeds the new allocated (3000)
    // and is clamped down; every ancestor aggregate follows.
    a.setSize(3000);
    r.sync();
    EXPECT_EQ(reclaimable_of(queue), 3000);
    EXPECT_EQ(reclaimable_of(limit), 3000);
    EXPECT_EQ(reclaimable_of(&t.scheduler), 3000);
}


/// If the spill-signaled victim declines (replies with zero reclaimable, WITHOUT decreasing), the episode
/// must not stall: the reply reopens the gate and the scheduler re-targets the next reclaimable allocation
/// while still over the soft limit. Without this `b` is never signaled and the test hangs on `waitSpills`.
TEST(SchedulerSpaceShared, SpillReSelectsWhenVictimDeclines)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 10000);

    SpillableAllocation a(queue, "a", 8000);
    SpillableAllocation b(queue, "b", 6000);
    a.reportReclaimable(8000);
    b.reportReclaimable(6000);

    // Over the soft limit (14000 > 10000): the largest reclaimable allocation (`a`) is asked to spill first.
    a.waitSpills(1);
    EXPECT_EQ(a.spillCount(), 1);
    EXPECT_EQ(b.spillCount(), 0); // only one spill is in flight at a time

    // `a` declines: it replies with nothing reclaimable and does NOT decrease. Still over the soft limit,
    // with `b` reclaimable, the scheduler must now ask `b` to spill instead of stalling on `a`.
    a.finishSpill(0);
    b.waitSpills(1);
    EXPECT_EQ(b.spillCount(), 1);
    EXPECT_EQ(b.lastSpillAtLeast(), 4000); // need = allocated(14000) - soft(10000)
    EXPECT_EQ(a.spillCount(), 1); // `a` is not re-signaled — it has nothing reclaimable
}


/// Pins the spill-gate semantics: at most one spill request is outstanding under the limit at a time, and
/// the gate is held until the victim REPLIES via `finishSpill` — unrelated activity (decreases by other
/// allocations, new reclaimable reports, growth) does not reopen it. Once the reply arrives, the next
/// check signals the then-current extremum with the then-current excess.
TEST(SchedulerSpaceShared, SpillGateHeldUntilVictimReply)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 10000);

    SpillableAllocation a(queue, "a", 8000); // will be the first victim
    SpillableAllocation b(queue, "b", 6000); // never reports reclaimable (unreclaimable)
    SpillableAllocation c(queue, "c", 7000); // will outgrow `a` and become the next extremum

    // Nothing reclaimable yet: over the soft limit (21000 > 10000) but fail-close, no signals.
    r.sync();
    EXPECT_EQ(a.spillCount() + b.spillCount() + c.spillCount(), 0u);

    // First report opens the episode: `a` is the only reclaimable allocation and gets signaled.
    a.reportReclaimable(8000);
    a.waitSpills(1);
    EXPECT_EQ(a.lastSpillAtLeast(), 11000); // need = allocated(21000) - soft(10000)

    // While `a`'s request is outstanding, more reclaimable appears, `c` grows past `a`, and an UNRELATED
    // allocation (`b`) shrinks. None of that is the victim's reply: the gate stays closed, no new signals.
    c.reportReclaimable(7000);
    c.setSize(9000);
    b.setSize(3000);
    r.sync();
    EXPECT_EQ(c.spillCount(), 0u);
    EXPECT_EQ(a.spillCount(), 1u);

    // The victim replies after spilling: `a` shrinks by 5000 and finishes. Only now does the scheduler
    // re-evaluate — and signals the new extremum `c` (fair_key 9000 > 3000) with the fresh excess.
    a.setSize(3000);
    a.finishSpill(3000);
    c.waitSpills(1);
    EXPECT_EQ(c.lastSpillAtLeast(), 5000); // need = allocated(3000+3000+9000) - soft(10000)
    EXPECT_EQ(a.spillCount(), 1u); // the reply closed `a`'s episode; `a` is not re-signaled
}


/// Pins the deferred re-evaluation: the victim issues its decrease and the reply in one go (the reply
/// reaches the scheduler while the decrease is still pending), so the re-evaluation must wait for the
/// decrease approval and use the updated `allocated` — not re-signal immediately with the stale excess.
TEST(SchedulerSpaceShared, SpillReplyDeferredToPendingDecrease)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 10000);

    SpillableAllocation a(queue, "a", 15000);
    a.reportReclaimable(15000);
    a.waitSpills(1);
    EXPECT_EQ(a.lastSpillAtLeast(), 5000); // need = allocated(15000) - soft(10000)

    // The victim spills 3000 and replies without waiting for the decrease approval: both arrive in one
    // activation. With the stale `allocated` (15000) the need would be 5000; the correct need after the
    // decrease is applied (12000) is 2000.
    a.spillAndFinish(/*spilled_bytes=*/ 3000, /*reclaimable_total=*/ 12000);
    a.waitSpills(2);
    EXPECT_EQ(a.lastSpillAtLeast(), 2000); // re-evaluated AFTER the decrease, never with the stale excess

    // Finish the episode: spill the rest of the excess; once under the soft limit no further signal comes.
    a.spillAndFinish(/*spilled_bytes=*/ 2000, /*reclaimable_total=*/ 10000);
    r.sync();
    size_t settled = a.spillCount();
    r.sync();
    EXPECT_EQ(a.spillCount(), settled);
    EXPECT_EQ(settled, 2u); // allocated(10000) <= soft(10000): episode over
}


/// No spill signal may be issued while a decrease is pending under the limit: the reply reopens the gate,
/// but until the victim's decrease is approved, `allocated` still contains the released memory. A trigger
/// arriving in that window from elsewhere (here: a reclaimable report from a sibling queue, processed as an
/// event BEFORE the decrease approval) must not evaluate the soft limit against the stale size — the
/// evaluation belongs to the approval's trailing check.
TEST(SchedulerSpaceShared, NoSpillSignalWhileVictimDecreaseIsPending)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    FairAllocation * fair = r.addFair("/fair", limit);
    AllocationQueue * q1 = r.addQueueUnder("/fair/q1", fair);
    AllocationQueue * q2 = r.addQueueUnder("/fair/q2", fair);
    r.registerResource();
    r.setSoftLimit(limit, 10000);

    SpillableAllocation a(q1, "a", 15000);
    SpillableAllocation x(q2, "x", 1000);
    a.reportReclaimable(15000);
    a.waitSpills(1);
    EXPECT_EQ(a.lastSpillAtLeast(), 6000); // need = allocated(16000) - soft(10000)

    // Park the scheduler so that the victim's reply and the sibling's report are both queued as events
    // ahead of the decrease approval (events are processed with priority over approvals).
    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    a.spillAndFinish(/*spilled_bytes=*/ 5000, /*reclaimable_total=*/ 10000); // reply + pending decrease
    x.reportReclaimable(1000); // a separate activation, processed after the reply but before the approval

    release.set_value();

    // The next signal must be computed only after the victim's decrease is applied:
    // need = allocated(16000 - 5000) - soft(10000) = 1000, and the victim is still the extremum.
    // Evaluating in the window instead would signal with the stale excess of 6000.
    a.waitSpills(2);
    EXPECT_EQ(a.lastSpillAtLeast(), 1000);
    EXPECT_EQ(x.spillCount(), 0u);
}


/// A victim that is removed mid-spill (query killed or cancelled) must not leave the gate closed forever:
/// its removal is treated as the reply, and the next reclaimable allocation is signaled if the subtree is
/// still over the soft limit.
TEST(SchedulerSpaceShared, SpillGateReopensWhenVictimRemoved)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 100000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();
    r.setSoftLimit(limit, 5000);

    SpillableAllocation b(queue, "b", 7000); // will be signaled after the victim disappears

    {
        SpillableAllocation a(queue, "a", 8000);
        a.reportReclaimable(8000);
        a.waitSpills(1);
        EXPECT_EQ(a.lastSpillAtLeast(), 10000); // need = allocated(15000) - soft(5000)

        b.reportReclaimable(7000); // gate is held by `a`: no signal for `b`
        r.sync();
        EXPECT_EQ(b.spillCount(), 0u);
        // `a` is destroyed here without ever replying — the removal is the implicit reply.
    }

    b.waitSpills(1);
    EXPECT_EQ(b.lastSpillAtLeast(), 2000); // need = allocated(7000) - soft(5000)
}


/// Concurrency stress for the reclaimable/spill machinery: many query threads churn allocation sizes and
/// reclaimable reports across sibling queues while the workload sits around a small soft limit, so spill
/// signals fire concurrently with increases, decreases, re-keying, clamping and removals; meanwhile the
/// soft limit itself is repeatedly moved. Run under ThreadSanitizer this exercises every cross-thread path
/// of the feature: `setReclaimable` vs approvals, `checkSoftLimit` from all of its trigger sites, and
/// `spillAllocation` callbacks racing allocation owners.
TEST(SchedulerSpaceShared, ConcurrentSpillChurn)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    AllocationLimit * limit = r.addLimit("/", 1000000000); // huge hard limit: no kills, only spills
    FairAllocation * fair = r.addFair("/fair", limit);
    constexpr size_t num_queues = 4;
    std::vector<AllocationQueue *> queues;
    for (size_t i = 0; i < num_queues; ++i)
        queues.push_back(r.addQueueUnder(fmt::format("/fair/q{}", i), fair));
    r.registerResource();
    r.setSoftLimit(limit, 1000); // small soft limit: almost always exceeded while threads run

    constexpr size_t num_threads = 8;
    constexpr size_t iterations = 50;
    std::atomic<size_t> total_spills{0};
    std::barrier<> start_barrier(num_threads + 1);

    std::vector<std::thread> threads;
    for (size_t th = 0; th < num_threads; ++th)
    {
        threads.emplace_back([&, th]
        {
            start_barrier.arrive_and_wait();
            AllocationQueue * queue = queues[th % num_queues];
            for (size_t i = 0; i < iterations; ++i)
            {
                SpillableAllocation a(queue, fmt::format("a_{}_{}", th, i), 5000 + th * 100);
                a.reportReclaimable(3000);
                a.setSize(8000 + i);
                a.reportReclaimable(6000);
                // Reply together with a pending decrease (the deferred re-evaluation path), shrinking
                // below the last report to exercise the clamp as well.
                a.spillAndFinish(/*spilled_bytes=*/ 6000 + i, /*reclaimable_total=*/ 1500);
                a.reportReclaimable(0);
                a.finishSpill(1500); // a reply that changes nothing but reopens the gates
                total_spills += a.spillCount();
                // Allocation destroyed here: exercises removal while possibly spill-signaled
            }
        });
    }

    // Concurrently move the soft limit up and down (as `CREATE OR REPLACE WORKLOAD` would).
    std::thread toggler([&]
    {
        start_barrier.arrive_and_wait();
        for (size_t i = 0; i < iterations; ++i)
        {
            r.setSoftLimit(limit, 500 + (i % 7) * 1000);
            std::this_thread::yield();
        }
        r.setSoftLimit(limit, 1000);
    });

    for (auto & thread : threads)
        thread.join();
    toggler.join();

    // With the soft limit far below the working set, spill signals must have fired.
    EXPECT_GT(total_spills.load(), 0u);

    // After all allocations are gone, every aggregate must return to zero (nothing leaks in the sets
    // either: destructors would trip on still-linked intrusive hooks).
    std::promise<std::pair<ResourceCost, ResourceCost>> p;
    auto fut = p.get_future();
    t.scheduler.event_queue.enqueue([&] { p.set_value({t.scheduler.reclaimable, t.scheduler.allocated}); });
    auto [reclaimable, allocated] = fut.get();
    EXPECT_EQ(reclaimable, 0);
    EXPECT_EQ(allocated, 0);
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
        // EXPECT (not ASSERT): an early return here would skip set_value and deadlock fut.get() below,
        // turning a red test into a hung job. The check also keeps the loop from being optimized away.
        EXPECT_NE(sink, nullptr); // the tree is populated
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


/// A minimal allocation for driving the scheduler deterministically: requests are issued without waiting
/// (so several can be queued while the scheduler thread is parked) and kill signals are recorded. Lock
/// ordering mirrors `MemoryReservation`: AllocationQueue::mutex -> ManualAllocation::mutex.
struct ManualAllocation : public ResourceAllocation
{
    ManualAllocation(AllocationQueue * queue_, const String & name_, ResourceCost initial_size)
        : ResourceAllocation(*queue_, name_)
    {
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

    ~ManualAllocation() override
    {
        {
            std::unique_lock lock(mutex);
            if (removed || fail_reason)
                return;
        }
        queue.removeAllocation(*this);
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return removed || fail_reason; });
    }

    /// Requests an increase without waiting for the approval.
    void increaseAsync(ResourceCost size)
    {
        {
            std::unique_lock lock(mutex);
            increase_enqueued = true;
        }
        queue.increaseAllocation(*this, size);
    }

    /// Requests a decrease without waiting for the approval.
    void decreaseAsync(ResourceCost size)
    {
        {
            std::unique_lock lock(mutex);
            decrease_enqueued = true;
        }
        queue.decreaseAllocation(*this, size);
    }

    /// Waits until all requests issued so far are approved.
    void waitSynced()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return fail_reason || (!increase_enqueued && !decrease_enqueued); });
        if (fail_reason)
            std::rethrow_exception(fail_reason);
    }

    size_t killCount()
    {
        std::unique_lock lock(mutex);
        return kills;
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

    void killAllocation(const std::exception_ptr &) override
    {
        std::unique_lock lock(mutex);
        ++kills;
        cv.notify_all();
    }

    void spillAllocation(ResourceCost) override {} // never a spill victim: reports no reclaimable memory

    std::mutex mutex;
    std::condition_variable cv;
    std::exception_ptr fail_reason;
    bool increase_enqueued = false;
    bool decrease_enqueued = false;
    bool removed = false;
    size_t kills = 0;
    ResourceCost allocated_size = 0;
};


/// An eviction must not be issued while a decrease is pending under the limit: `allocated` still contains
/// memory that is about to be released, so the kill may be unnecessary. Here `b`'s decrease frees exactly
/// the room `a`'s increase needs; both requests reach the scheduler in one activation (the scheduler
/// thread is parked while they are issued), so the eviction decision runs while the decrease is pending.
/// Without the fix the largest allocation (`a`) is killed even though no eviction was needed at all.
TEST(SchedulerSpaceShared, NoKillWhileDecreaseIsPending)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation a(queue, "a", 7000);
    ManualAllocation b(queue, "b", 3000);
    // At the limit exactly (10000): any increase overflows and triggers the eviction decision.

    // Park the scheduler so both requests below are queued before either is processed: they arrive in
    // one activation, and the eviction decision for the increase runs while the decrease is pending.
    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    b.decreaseAsync(3000); // releases enough room for the increase below
    a.increaseAsync(2000); // 10000 + 2000 > 10000: over the limit until b's decrease is applied

    release.set_value();

    // The decrease is approved first (total 7000), after which the increase fits (9000 <= 10000):
    // nobody needs to be evicted. Without the fix, `a` (the largest) is killed by its own increase
    // even though its increase is then approved anyway once the decrease lands.
    a.waitSynced();
    b.waitSynced();
    ASSERT_EQ(a.killCount(), 0u);
    ASSERT_EQ(b.killCount(), 0u);
    EXPECT_EQ(a.size(), 9000);
    EXPECT_EQ(b.size(), 0);
}
