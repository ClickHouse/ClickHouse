#include <gtest/gtest.h>

#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/MemoryTracker.h>

#include <barrier>
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
