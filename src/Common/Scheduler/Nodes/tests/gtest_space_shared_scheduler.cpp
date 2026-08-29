#include <gtest/gtest.h>

#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/FairAllocation.h>
#include <Common/Scheduler/Nodes/SpaceShared/PrecedenceAllocation.h>
#include <Common/Scheduler/Nodes/WorkloadNode.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/MemoryTracker.h>
#include <Processors/IProcessor.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <future>
#include <functional>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
#include <utility>
#include <vector>

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
        if (!root_node)
            return;

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


/// Reads scheduler-thread-only accounting without racing the scheduler thread. The deadline makes
/// failures in the event hand-off visible without turning a focused regression into a suite hang.
struct SpaceSharedSchedulerSnapshot
{
    ResourceCost root_allocated = 0;
    ResourceCost queue_allocated = 0;
    UInt64 queue_increases = 0;
    UInt64 queue_decreases = 0;
    bool increase_pending = false;
    bool decrease_pending = false;
};

static std::optional<SpaceSharedSchedulerSnapshot> getSchedulerSnapshot(
    SpaceSharedTest & t, AllocationQueue & queue, std::chrono::milliseconds timeout = std::chrono::seconds(5))
{
    auto promise = std::make_shared<std::promise<SpaceSharedSchedulerSnapshot>>();
    auto future = promise->get_future();
    t.scheduler.event_queue.enqueue([&t, &queue, promise]
    {
        promise->set_value({
            .root_allocated = t.scheduler.allocated,
            .queue_allocated = queue.allocated,
            .queue_increases = queue.increases,
            .queue_decreases = queue.decreases,
            .increase_pending = t.scheduler.increase != nullptr,
            .decrease_pending = t.scheduler.decrease != nullptr,
        });
    });

    if (future.wait_for(timeout) != std::future_status::ready)
        return std::nullopt;
    return future.get();
}


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


/// The first executor which reaches a reservation defines the dependency graph controller.
/// A nested executor may propose a different thread-local controller, but it must join the first
/// processor census instead of splitting one graph into independent recovery episodes.
TEST(SchedulerSpaceShared, ReservationKeepsFirstDependencyGraphController)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;
    MemoryReservation reservation(link, "reservation", 0);

    auto first = std::make_shared<MemorySpillScheduler>(/*enable_=*/ false);
    auto nested = std::make_shared<MemorySpillScheduler>(/*enable_=*/ false);
    EXPECT_EQ(reservation.bindMemorySpillScheduler(first).get(), first.get());
    EXPECT_EQ(reservation.bindMemorySpillScheduler(nested).get(), first.get());
    EXPECT_EQ(reservation.bindMemorySpillScheduler(nullptr).get(), first.get())
        << "An executor without its own ThreadGroup must still recover the canonical controller";
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

    // Test 1: Increasing memory usage progressively. These legacy growth cases reset the tracker
    // without another sync; retained-slack and final-release behavior is covered explicitly below.
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


/// Dropping below the charged size keeps query-local slack instead of immediately publishing a
/// decrease. Regrowth within that slack must therefore need neither a decrease nor a new increase.
/// This is the basic locality guarantee behind hierarchical release credit.
TEST(SchedulerSpaceShared, ReservationReusesRetainedUnusedCapacity)
{
    SpaceSharedTest t;

    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;
    MemoryTracker tracker;

    SpaceSharedSchedulerSnapshot charged;
    {
        MemoryReservation reservation(link, "reservation", 0);
        tracker.adjustWithUntrackedMemory(8000);
        reservation.syncWithMemoryTracker(&tracker);

        auto snapshot = getSchedulerSnapshot(t, *queue);
        ASSERT_TRUE(snapshot.has_value());
        charged = *snapshot;
        ASSERT_EQ(charged.root_allocated, 8000);
        ASSERT_EQ(charged.queue_allocated, 8000);

        tracker.adjustWithUntrackedMemory(-2000); // 6000 used, but 8000 remains charged locally.
        reservation.syncWithMemoryTracker(&tracker);
        tracker.adjustWithUntrackedMemory(1000); // Reuse 1000 of the retained 2000.
        reservation.syncWithMemoryTracker(&tracker);

        snapshot = getSchedulerSnapshot(t, *queue);
        ASSERT_TRUE(snapshot.has_value());
        EXPECT_EQ(snapshot->root_allocated, 8000);
        EXPECT_EQ(snapshot->queue_allocated, 8000);
        EXPECT_EQ(snapshot->queue_increases, charged.queue_increases);
        EXPECT_EQ(snapshot->queue_decreases, charged.queue_decreases);
        EXPECT_FALSE(snapshot->increase_pending);
        EXPECT_FALSE(snapshot->decrease_pending);

        tracker.adjustWithUntrackedMemory(-7000);
    }

    auto released = getSchedulerSnapshot(t, *queue);
    ASSERT_TRUE(released.has_value());
    EXPECT_EQ(released->root_allocated, 0);
    EXPECT_EQ(released->queue_allocated, 0);
    EXPECT_EQ(released->queue_decreases, charged.queue_decreases + 1);
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
    ManualAllocation(IAllocationQueue * queue_, const String & name_, ResourceCost initial_size, bool wait_for_admission = true)
        : ResourceAllocation(*queue_, name_)
    {
        if (initial_size > 0)
            increase_enqueued = true;
        queue.insertAllocation(*this, initial_size); // scheduler thread may call back after this
        if (initial_size > 0 && wait_for_admission) // Block until admitted, like MemoryReservation with reserve_memory > 0
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

    /// Requests removal without waiting. Tests use this from a scheduler event queued while an
    /// approval callback is gated, so cancellation deterministically precedes follow-on work.
    void removeAsync()
    {
        queue.removeAllocation(*this);
    }

    bool waitRemovedFor(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [this] { return removed || fail_reason; });
    }

    /// Waits until all requests issued so far are approved.
    void waitSynced()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return fail_reason || (!increase_enqueued && !decrease_enqueued); });
        if (fail_reason)
            std::rethrow_exception(fail_reason);
    }

    bool waitSyncedFor(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        const bool synced = cv.wait_for(lock, timeout, [this]
        {
            return fail_reason || (!increase_enqueued && !decrease_enqueued);
        });
        if (fail_reason)
            std::rethrow_exception(fail_reason);
        return synced;
    }

    size_t killCount()
    {
        std::unique_lock lock(mutex);
        return kills;
    }

    void waitKills(size_t count)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return kills >= count; });
    }

    bool waitKillsFor(size_t count, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return kills >= count; });
    }

    ResourceCost size()
    {
        std::unique_lock lock(mutex);
        return allocated_size;
    }

    void protectAfterPressureRounds(size_t rounds)
    {
        std::unique_lock lock(mutex);
        protection_round = rounds;
    }

    void waitPressureCount(size_t count)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return total_pressure_events >= count; });
    }

    bool waitPressureCountFor(size_t count, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return total_pressure_events >= count; });
    }

    size_t pressureCount()
    {
        std::unique_lock lock(mutex);
        return total_pressure_events;
    }

    void recoveryCheckpoint()
    {
        queue.notifyRecoveryProgress(*this, 0);
    }

    void runOnNextPressure(std::function<void()> callback)
    {
        std::unique_lock lock(mutex);
        next_pressure_callback = std::move(callback);
    }

    void reconcilePendingIncreaseTo(ResourceCost size)
    {
        std::unique_lock lock(mutex);
        reconciled_increase_size = size;
    }

    void runOnNextApproval(std::function<void()> callback)
    {
        std::unique_lock lock(mutex);
        next_approval_callback = std::move(callback);
    }

    void runOnNextDecreaseApproval(std::function<void()> callback)
    {
        std::unique_lock lock(mutex);
        next_decrease_approval_callback = std::move(callback);
    }

    void offerUnusedCapacity(ResourceCost size)
    {
        std::unique_lock lock(mutex);
        reclaimable_capacity = size;
    }

    void runOnNextReclaim(std::function<void()> callback)
    {
        std::unique_lock lock(mutex);
        next_reclaim_callback = std::move(callback);
    }

private: // interaction with the scheduler thread
    GrowthPressureAction onGrowthPressure() override
    {
        std::function<void()> callback;
        GrowthPressureAction action = GrowthPressureAction::Yield;
        {
            std::unique_lock lock(mutex);
            ++current_pressure_round;
            ++total_pressure_events;
            const bool protect = protection_round != 0 && current_pressure_round >= protection_round;
            recovery_active = !protect;
            action = protect ? GrowthPressureAction::Protect : GrowthPressureAction::Yield;
            callback = std::move(next_pressure_callback);
            cv.notify_all();
        }

        /// Run outside the allocation mutex so a query-side recovery callback can safely enter the
        /// queue. This deliberately models the real hand-off from scheduler pressure notification
        /// to a pipeline worker.
        if (callback)
            callback();
        return action;
    }

    void onGrowthPressureResolved() override
    {
        std::unique_lock lock(mutex);
        current_pressure_round = 0;
        recovery_active = false;
        cv.notify_all();
    }

    bool isGrowthRecoveryActive() override
    {
        std::unique_lock lock(mutex);
        return recovery_active;
    }

    ResourceCost reconcilePendingIncrease(ResourceCost, ResourceCost requested_size) override
    {
        std::unique_lock lock(mutex);
        return reconciled_increase_size.value_or(requested_size);
    }

    ResourceCost reclaimUnusedCapacity(ResourceCost max_size) override
    {
        ResourceCost reclaimed = 0;
        std::function<void()> callback;
        {
            std::unique_lock lock(mutex);
            reclaimed = std::min(max_size, reclaimable_capacity);
            reclaimable_capacity -= reclaimed;
            callback = std::move(next_reclaim_callback);
        }
        /// AllocationQueue still owns this donor queue's mutex. The callback may only publish work
        /// to a different queue; it exists solely to make cross-queue release ordering deterministic.
        if (callback)
            callback();
        return reclaimed;
    }

    void increaseCancelled() override
    {
        std::unique_lock lock(mutex);
        increase_enqueued = false;
        cv.notify_all();
    }

    void increaseApproved(const IncreaseRequest & increase) override
    {
        std::function<void()> callback;
        {
            std::unique_lock lock(mutex);
            allocated_size += increase.size;
            increase_enqueued = false;
            callback = std::move(next_approval_callback);
            cv.notify_all();
        }
        if (callback)
            callback();
    }

    void decreaseApproved(const DecreaseRequest & decrease) override
    {
        std::function<void()> callback;
        {
            std::unique_lock lock(mutex);
            allocated_size -= decrease.size;
            decrease_enqueued = false;
            if (decrease.removing_allocation)
                removed = true;
            callback = std::move(next_decrease_approval_callback);
            cv.notify_all();
        }
        if (callback)
            callback();
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
    size_t protection_round = 1;
    size_t current_pressure_round = 0;
    size_t total_pressure_events = 0;
    bool recovery_active = false;
    std::function<void()> next_pressure_callback;
    std::function<void()> next_reclaim_callback;
    std::function<void()> next_approval_callback;
    std::function<void()> next_decrease_approval_callback;
    std::optional<ResourceCost> reconciled_increase_size;
    ResourceCost reclaimable_capacity = 0;
};


/// A request may already be parked when another allocation first develops unused charged capacity.
/// The late release notification must wake the constrained limit, reclaim exactly the shortage through
/// an ordinary decrease, and resume the request without eviction.
TEST(SchedulerSpaceShared, LateUnusedCapacityWakesBlockedGrowth)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;
    MemoryTracker donor_tracker;
    MemoryReservation donor(link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);

    ManualAllocation requester(queue, "requester", 2000);
    requester.protectAfterPressureRounds(2);
    requester.increaseAsync(2000); // 8000 + 2000 + 2000 exceeds the limit.
    ASSERT_TRUE(requester.waitPressureCountFor(1, std::chrono::seconds(5)));
    EXPECT_EQ(requester.size(), 2000);
    EXPECT_EQ(requester.killCount(), 0u);
    auto blocked = getSchedulerSnapshot(t, *queue);
    ASSERT_TRUE(blocked.has_value());
    ASSERT_EQ(blocked->root_allocated, 10000);

    donor_tracker.adjustWithUntrackedMemory(-2000); // Slack appears after requester was parked.
    donor.syncWithMemoryTracker(&donor_tracker);

    ASSERT_TRUE(requester.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(requester.size(), 4000);
    EXPECT_EQ(requester.killCount(), 0u);

    auto snapshot = getSchedulerSnapshot(t, *queue);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 10000);
    EXPECT_EQ(snapshot->queue_allocated, 10000);
    EXPECT_EQ(snapshot->queue_decreases, blocked->queue_decreases + 1);
    EXPECT_FALSE(snapshot->increase_pending);
    EXPECT_FALSE(snapshot->decrease_pending);

    /// The donor remains alive with its actual 6000 bytes after the scheduler reclaimed only its
    /// unused 2000-byte slack.
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));
    donor_tracker.adjustWithUntrackedMemory(-6000);
}


template <typename Policy>
void unusedCapacityIsReclaimedThroughPolicy()
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<Policy>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    SchedulerNodeInfo donor_info;
    donor_info.setPrecedence(1);
    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, donor_info);
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    policy->attachChild(donor_queue);

    SchedulerNodeInfo requester_info;
    requester_info.setPrecedence(0);
    auto requester_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, requester_info);
    requester_queue->basename = "requester";
    AllocationQueue * requester_queue_ptr = requester_queue.get();
    policy->attachChild(requester_queue);

    r.root_node = limit;
    requester_queue.reset();
    donor_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = donor_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);

    /// Publish unused capacity before there is contention. It stays local until a real request needs it.
    donor_tracker.adjustWithUntrackedMemory(-2000);
    donor.syncWithMemoryTracker(&donor_tracker);
    auto retained = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 8000);
    ASSERT_EQ(retained->queue_allocated, 8000);

    ManualAllocation requester(requester_queue_ptr, "requester", 2000);
    requester.increaseAsync(2000);
    ASSERT_TRUE(requester.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(requester.size(), 4000);
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    auto snapshot = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 10000);
    EXPECT_EQ(snapshot->queue_allocated, 6000);
    EXPECT_EQ(snapshot->queue_decreases, retained->queue_decreases + 1);
    EXPECT_FALSE(snapshot->increase_pending);
    EXPECT_FALSE(snapshot->decrease_pending);

    donor_tracker.adjustWithUntrackedMemory(-6000);
}


TEST(SchedulerSpaceShared, UnusedCapacityIsReclaimedThroughFairPolicy)
{
    unusedCapacityIsReclaimedThroughPolicy<FairAllocation>();
}


TEST(SchedulerSpaceShared, UnusedCapacityIsReclaimedThroughPrecedencePolicy)
{
    unusedCapacityIsReclaimedThroughPolicy<PrecedenceAllocation>();
}


/// Reclaim commits bytes before its ordinary decrease is approved. A query thread that regrows in
/// that interval may reuse only the unclaimed remainder; it must not double-spend the committed bytes.
TEST(SchedulerSpaceShared, DonorRegrowthUsesOnlyUnclaimedCapacity)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    limit->attachChild(policy);

    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    policy->attachChild(donor_queue);

    auto requester_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    requester_queue->basename = "requester";
    AllocationQueue * requester_queue_ptr = requester_queue.get();
    policy->attachChild(requester_queue);

    r.root_node = limit;
    requester_queue.reset();
    donor_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = donor_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-2000); // 2000 reusable bytes remain charged.
    donor.syncWithMemoryTracker(&donor_tracker);

    ManualAllocation requester(requester_queue_ptr, "requester", 2000);
    auto retained = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(retained.has_value());

    /// Keep approvals stopped while normal node activations publish the donor's reclaim decrease.
    /// The self-reenqueuing watcher runs on the scheduler thread and therefore inspects `decrease`
    /// without a data race. EventQueue is drained before approvals, so blocking it at that point
    /// creates the exact committed-but-not-approved interval without sleeps.
    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the deterministic pre-request gate";
    }

    requester.increaseAsync(1000); // Deficit is exactly 1000; leave 1000 donor slack unclaimed.

    std::promise<void> reclaim_published;
    auto reclaim_published_future = reclaim_published.get_future();
    std::promise<void> release_watcher;
    auto release_watcher_future = release_watcher.get_future().share();
    std::promise<void> watcher_done;
    auto watcher_done_future = watcher_done.get_future();
    std::atomic<bool> stop_watcher{false};
    auto watcher = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weak_watcher = watcher;
    *watcher = [&, weak_watcher]
    {
        if (stop_watcher.load())
        {
            watcher_done.set_value();
            return;
        }
        if (t.scheduler.decrease && t.scheduler.decrease->allocation.id == "donor")
        {
            reclaim_published.set_value();
            release_watcher_future.wait();
            watcher_done.set_value();
            return;
        }
        if (auto next = weak_watcher.lock())
            t.scheduler.event_queue.enqueue(EventQueue::Task(*next));
    };
    t.scheduler.event_queue.enqueue(EventQueue::Task(*watcher));
    start_scheduler.set_value();

    const auto reclaim_status = reclaim_published_future.wait_for(std::chrono::seconds(5));
    if (reclaim_status != std::future_status::ready)
    {
        stop_watcher = true;
        release_watcher.set_value();
        EXPECT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        FAIL() << "The donor reclaim decrease was not published before approval";
    }

    /// One of the two unused kilobytes is now irrevocably claimed. Regrowth to 7000 consumes only
    /// the remaining kilobyte and must return while the scheduler is still parked.
    donor_tracker.adjustWithUntrackedMemory(1000);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    release_watcher.set_value();
    ASSERT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_TRUE(requester.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(requester.size(), 3000);
    EXPECT_EQ(requester.killCount(), 0u);

    auto snapshot = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 10000);
    EXPECT_EQ(snapshot->queue_allocated, 7000);
    EXPECT_EQ(snapshot->queue_decreases, retained->queue_decreases + 1);

    donor_tracker.adjustWithUntrackedMemory(-7000);
}


/// A worker may release memory while another worker is blocked waiting for this reservation's
/// increase approval. The waiting sync must re-snapshot actual demand before it returns; otherwise
/// the just-approved excess remains falsely busy and cannot be reclaimed by a real contender.
TEST(SchedulerSpaceShared, ReleaseDuringPendingReservationIncreaseBecomesReclaimable)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    limit->attachChild(policy);

    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    policy->attachChild(donor_queue);

    auto requester_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    requester_queue->basename = "requester";
    AllocationQueue * requester_queue_ptr = requester_queue.get();
    policy->attachChild(requester_queue);

    r.root_node = limit;
    requester_queue.reset();
    donor_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = donor_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(6000);
    donor.syncWithMemoryTracker(&donor_tracker);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the pre-increase gate";
    }

    donor_tracker.adjustWithUntrackedMemory(3000); // The sync below asks to grow from 6000 to 9000.
    auto sync_future = std::async(std::launch::async, [&]
    {
        donor.syncWithMemoryTracker(&donor_tracker);
    });

    /// Stop the scheduler after the increase has reached the root but before it can be approved.
    /// Scheduler events are drained before approvals, so this self-reenqueuing event observes the
    /// exact in-flight interval without a timing delay.
    std::promise<void> increase_published;
    auto increase_published_future = increase_published.get_future();
    std::promise<void> release_approval;
    auto release_approval_future = release_approval.get_future().share();
    std::promise<void> watcher_done;
    auto watcher_done_future = watcher_done.get_future();
    std::atomic<bool> stop_watcher{false};
    auto watcher = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weak_watcher = watcher;
    *watcher = [&, weak_watcher]
    {
        if (stop_watcher.load())
        {
            watcher_done.set_value();
            return;
        }
        if (t.scheduler.increase && t.scheduler.increase->allocation.id == "donor")
        {
            increase_published.set_value();
            release_approval_future.wait();
            watcher_done.set_value();
            return;
        }
        if (auto next = weak_watcher.lock())
            t.scheduler.event_queue.enqueue(EventQueue::Task(*next));
    };
    t.scheduler.event_queue.enqueue(EventQueue::Task(*watcher));
    start_scheduler.set_value();

    if (increase_published_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        stop_watcher = true;
        release_approval.set_value();
        EXPECT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        FAIL() << "The reservation increase was not observed before approval";
    }

    donor_tracker.adjustWithUntrackedMemory(-2000); // Actual demand falls to 7000 while +3000 waits.
    release_approval.set_value();
    ASSERT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(sync_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_NO_THROW(sync_future.get());

    auto retained = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 9000);
    ASSERT_EQ(retained->queue_allocated, 9000);

    /// The resnapshot records 2000 bytes of local slack. A 2000-byte contender needs exactly
    /// 1000 of it, so the donor should surrender only that amount and remain safely above its
    /// actual 7000-byte demand.
    ManualAllocation requester(requester_queue_ptr, "requester", 2000, /* wait_for_admission = */ false);
    ASSERT_TRUE(requester.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(requester.size(), 2000);
    EXPECT_EQ(requester.killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    auto reclaimed = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(reclaimed.has_value());
    EXPECT_EQ(reclaimed->root_allocated, 10000);
    EXPECT_EQ(reclaimed->queue_allocated, 8000);
    EXPECT_EQ(reclaimed->queue_decreases, retained->queue_decreases + 1);
    EXPECT_FALSE(reclaimed->increase_pending);
    EXPECT_FALSE(reclaimed->decrease_pending);

    donor_tracker.adjustWithUntrackedMemory(-7000);
}


/// Retained capacity belongs to the nearest dependency subgraph first. An older request outside
/// that subgraph must not claim its slack while a fitting local request can immediately reuse it.
/// The handoff lasts for exactly one local selection; unused remainder then propagates outward.
TEST(SchedulerSpaceShared, InnerSubgraphGetsOneLocalSelectionBeforeOuterContender)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    outer_limit->attachChild(policy);

    /// Both limits need the same first 1000-byte release. It may fund only the nearest graph;
    /// the outer request must wait for a second distinct release event. Separate donor and local
    /// leaves prove that the handoff belongs to the dependency subgraph, not merely one queue.
    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    inner_limit->basename = "inner";
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);
    inner_limit->attachChild(inner_policy);
    policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    donor_queue.reset();
    local_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = donor_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-2000); // Inner graph owns 2000 of reusable slack.
    donor.syncWithMemoryTracker(&donor_tracker);

    auto retained = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 8000);
    ASSERT_EQ(retained->queue_allocated, 8000);

    /// Queue the external request first while the scheduler is parked, then publish the local
    /// request in the same scheduling batch. Correct hierarchy handling gives the inner limit the
    /// first opportunity to consume its own slack instead of donating it to the outer waiter.
    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the deterministic pre-request gate";
    }

    /// A zero-size allocation's first growth is `Initial`, so unlike a pending admission it may
    /// select a victim in another Fair child. This makes the ordering assertion meaningful: the
    /// inner graph must get its local reclaim round before the outer request may evict its donor.
    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(1000);
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    auto local_approved = std::make_shared<std::promise<void>>();
    auto local_approved_future = local_approved->get_future();
    auto release_local_approval = std::make_shared<std::promise<void>>();
    auto release_local_approval_future = release_local_approval->get_future().share();
    local->runOnNextApproval([local_approved, release_local_approval_future]
    {
        local_approved->set_value();
        release_local_approval_future.wait();
    });
    start_scheduler.set_value();

    if (local_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_local_approval->set_value();
        FAIL() << "The inner request did not receive the subgraph's retained capacity first";
    }
    EXPECT_EQ(local->size(), 1000);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_EQ(external->size(), 0);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    /// The first exact release funded the local request and cannot also fund the outer one. Once
    /// that one-selection handoff is spent, a second retained kilobyte may propagate outward.
    release_local_approval->set_value();
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(external->size(), 1000);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(local->size(), 1000);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    auto propagated = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(propagated.has_value());
    EXPECT_EQ(propagated->root_allocated, 8000);
    EXPECT_EQ(propagated->queue_allocated, 6000);
    EXPECT_EQ(propagated->queue_decreases, retained->queue_decreases + 2);
    EXPECT_FALSE(propagated->increase_pending);
    EXPECT_FALSE(propagated->decrease_pending);

    local.reset();
    external.reset();

    donor_tracker.adjustWithUntrackedMemory(-6000);
}


/// A graph's current Fair head may be larger than the exact release while a later sibling fits.
/// The release is offered once in normal Fair order, so the first fitting local request receives it
/// before an older contender outside the graph. The outer request needs a second release event.
TEST(SchedulerSpaceShared, ReleasedCapacitySearchesPastOversizedFairHead)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    auto nested_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    nested_policy->basename = "nested_fair";
    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    nested_policy->attachChild(donor_queue);
    auto oversized_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    oversized_queue->basename = "oversized";
    AllocationQueue * oversized_queue_ptr = oversized_queue.get();
    nested_policy->attachChild(oversized_queue);
    auto fitting_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    fitting_queue->basename = "fitting";
    AllocationQueue * fitting_queue_ptr = fitting_queue.get();
    nested_policy->attachChild(fitting_queue);
    inner_policy->attachChild(nested_policy);
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    fitting_queue.reset();
    oversized_queue.reset();
    donor_queue.reset();
    nested_policy.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    auto oversized = std::make_unique<ManualAllocation>(oversized_queue_ptr, "oversized", 1);
    oversized->decreaseAsync(1);
    ASSERT_TRUE(oversized->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_EQ(oversized->size(), 0);

    ResourceLink donor_link;
    donor_link.allocation_queue = donor_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-2000);
    donor.syncWithMemoryTracker(&donor_tracker); // Keep two exact 1000-byte release events locally reusable.

    auto retained = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 8000);
    ASSERT_EQ(retained->queue_allocated, 8000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the Fair handoff gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(1000);
    oversized->increaseAsync(2000); // Admitted growth is the ordinary Fair head.
    auto fitting = std::make_unique<ManualAllocation>(
        fitting_queue_ptr, "fitting", 1000, /* wait_for_admission = */ false);

    auto fitting_approved = std::make_shared<std::promise<void>>();
    auto fitting_approved_future = fitting_approved->get_future();
    std::promise<void> release_fitting_approval;
    auto release_fitting_approval_future = release_fitting_approval.get_future().share();
    fitting->runOnNextApproval([fitting_approved, release_fitting_approval_future]
    {
        fitting_approved->set_value();
        release_fitting_approval_future.wait();
    });
    start_scheduler.set_value();

    if (fitting_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_fitting_approval.set_value();
        FAIL() << "The exact release was donated past a fitting sibling behind the oversized Fair head";
    }
    EXPECT_EQ(fitting->size(), 1000);
    EXPECT_EQ(fitting->killCount(), 0u);
    EXPECT_EQ(oversized->size(), 0);
    EXPECT_EQ(oversized->killCount(), 0u);
    EXPECT_EQ(external->size(), 0);
    EXPECT_EQ(external->killCount(), 0u);

    oversized->removeAsync();
    release_fitting_approval.set_value();
    ASSERT_TRUE(oversized->waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(external->size(), 1000);
    EXPECT_EQ(external->killCount(), 0u);

    auto propagated = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(propagated.has_value());
    EXPECT_EQ(propagated->root_allocated, 8000);
    EXPECT_EQ(propagated->queue_allocated, 6000);
    EXPECT_EQ(propagated->queue_decreases, retained->queue_decreases + 2);
    EXPECT_FALSE(propagated->increase_pending);
    EXPECT_FALSE(propagated->decrease_pending);

    fitting.reset();
    oversized.reset();
    external.reset();
    donor_tracker.adjustWithUntrackedMemory(-6000);
}


/// The same exact handoff must cross the transparent workload wrappers used in production. The
/// dependency graph retains one release for fitting work below its own Fair policy; only the next
/// release propagates through the graph wrapper to an external sibling.
TEST(SchedulerSpaceShared, ReleasedCapacityTraversesWorkloadHierarchy)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    WorkloadSettings outer_settings;
    outer_settings.max_memory = 8000;
    auto outer = std::make_shared<SpaceSharedWorkloadNode>(
        t.scheduler.event_queue, outer_settings, CostUnit::MemoryByte, "memory");
    outer->basename = "all";

    WorkloadSettings graph_settings;
    graph_settings.max_memory = 10000;
    auto graph = std::make_shared<SpaceSharedWorkloadNode>(
        t.scheduler.event_queue, graph_settings, CostUnit::MemoryByte, "memory");
    graph->basename = "graph";

    WorkloadSettings leaf_settings;
    auto donor_workload = std::make_shared<SpaceSharedWorkloadNode>(
        t.scheduler.event_queue, leaf_settings, CostUnit::MemoryByte, "memory");
    donor_workload->basename = "donor";
    auto oversized_workload = std::make_shared<SpaceSharedWorkloadNode>(
        t.scheduler.event_queue, leaf_settings, CostUnit::MemoryByte, "memory");
    oversized_workload->basename = "oversized";
    auto fitting_workload = std::make_shared<SpaceSharedWorkloadNode>(
        t.scheduler.event_queue, leaf_settings, CostUnit::MemoryByte, "memory");
    fitting_workload->basename = "fitting";
    auto external_workload = std::make_shared<SpaceSharedWorkloadNode>(
        t.scheduler.event_queue, leaf_settings, CostUnit::MemoryByte, "memory");
    external_workload->basename = "external";

    graph->attachWorkloadChild(donor_workload);
    graph->attachWorkloadChild(oversized_workload);
    graph->attachWorkloadChild(fitting_workload);
    outer->attachWorkloadChild(graph);
    outer->attachWorkloadChild(external_workload);

    ResourceLink donor_link = donor_workload->getLink();
    ResourceLink oversized_link = oversized_workload->getLink();
    ResourceLink fitting_link = fitting_workload->getLink();
    ResourceLink external_link = external_workload->getLink();
    auto * donor_queue = static_cast<AllocationQueue *>(donor_link.allocation_queue);

    r.root_node = outer;
    external_workload.reset();
    fitting_workload.reset();
    oversized_workload.reset();
    donor_workload.reset();
    graph.reset();
    outer.reset();
    r.registerResource();

    auto oversized = std::make_unique<ManualAllocation>(oversized_link.allocation_queue, "oversized", 1);
    oversized->decreaseAsync(1);
    ASSERT_TRUE(oversized->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_EQ(oversized->size(), 0);

    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-2000);
    donor.syncWithMemoryTracker(&donor_tracker);

    auto retained = getSchedulerSnapshot(t, *donor_queue);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 8000);
    ASSERT_EQ(retained->queue_allocated, 8000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the workload-hierarchy handoff gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_link.allocation_queue, "external", 0);
    external->increaseAsync(1000);
    oversized->increaseAsync(2000); // Admitted growth is the ordinary Fair head.
    auto fitting = std::make_unique<ManualAllocation>(
        fitting_link.allocation_queue, "fitting", 1000, /* wait_for_admission = */ false);

    auto fitting_approved = std::make_shared<std::promise<void>>();
    auto fitting_approved_future = fitting_approved->get_future();
    std::promise<void> release_fitting_approval;
    auto release_fitting_approval_future = release_fitting_approval.get_future().share();
    fitting->runOnNextApproval([fitting_approved, release_fitting_approval_future]
    {
        fitting_approved->set_value();
        release_fitting_approval_future.wait();
    });
    start_scheduler.set_value();

    if (fitting_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_fitting_approval.set_value();
        FAIL() << "The graph wrapper donated capacity before its fitting descendant ran";
    }
    EXPECT_EQ(fitting->size(), 1000);
    EXPECT_EQ(fitting->killCount(), 0u);
    EXPECT_EQ(oversized->size(), 0);
    EXPECT_EQ(oversized->killCount(), 0u);
    EXPECT_EQ(external->size(), 0);
    EXPECT_EQ(external->killCount(), 0u);

    oversized->removeAsync();
    release_fitting_approval.set_value();
    ASSERT_TRUE(oversized->waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(external->size(), 1000);
    EXPECT_EQ(external->killCount(), 0u);

    auto propagated = getSchedulerSnapshot(t, *donor_queue);
    ASSERT_TRUE(propagated.has_value());
    EXPECT_EQ(propagated->root_allocated, 8000);
    EXPECT_EQ(propagated->queue_allocated, 6000);
    EXPECT_EQ(propagated->queue_decreases, retained->queue_decreases + 2);
    EXPECT_FALSE(propagated->increase_pending);
    EXPECT_FALSE(propagated->decrease_pending);

    fitting.reset();
    oversized.reset();
    external.reset();
    donor_tracker.adjustWithUntrackedMemory(-6000);
}


/// A single release can be partly consumed by its nearest subgraph without turning the remainder
/// into reserved credit. The unused aggregate capacity remains visible at the parent, where normal
/// park-and-search scheduling can give it to another fitting graph in the same release cycle.
TEST(SchedulerSpaceShared, UnusedReleaseRemainderPropagatesToParent)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto root_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    root_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto oversized_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    oversized_queue->basename = "oversized";
    AllocationQueue * oversized_queue_ptr = oversized_queue.get();
    outer_policy->attachChild(oversized_queue);
    auto fitting_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    fitting_queue->basename = "fitting";
    AllocationQueue * fitting_queue_ptr = fitting_queue.get();
    outer_policy->attachChild(fitting_queue);

    r.root_node = root_limit;
    fitting_queue.reset();
    oversized_queue.reset();
    local_queue.reset();
    donor_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    root_limit.reset();
    r.registerResource();

    ManualAllocation donor(donor_queue_ptr, "donor", 10000);
    donor.offerUnusedCapacity(2000);
    auto before = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(before.has_value());

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the single-release propagation gate";
    }

    auto oversized = std::make_unique<ManualAllocation>(oversized_queue_ptr, "oversized", 0);
    oversized->increaseAsync(2000);
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 500, /* wait_for_admission = */ false);
    auto fitting = std::make_unique<ManualAllocation>(
        fitting_queue_ptr, "fitting", 1500, /* wait_for_admission = */ false);

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::atomic<bool> first_recorded{false};
    auto record_first = [&](const String & id)
    {
        if (!first_recorded.exchange(true))
            first_approval.set_value(id);
    };
    local->runOnNextApproval([&] { record_first("local"); });
    fitting->runOnNextApproval([&] { record_first("fitting"); });
    start_scheduler.set_value();

    ASSERT_EQ(first_approval_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(first_approval_future.get(), "local");
    ASSERT_TRUE(local->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(fitting->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(local->size(), 500);
    EXPECT_EQ(fitting->size(), 1500);
    EXPECT_EQ(oversized->size(), 0);
    EXPECT_EQ(donor.size(), 8000);
    EXPECT_EQ(donor.killCount(), 0u);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_EQ(fitting->killCount(), 0u);
    EXPECT_EQ(oversized->killCount(), 0u);

    auto after = getSchedulerSnapshot(t, *donor_queue_ptr);
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->root_allocated, 10000);
    EXPECT_EQ(after->queue_allocated, 8000);
    EXPECT_EQ(after->queue_decreases, before->queue_decreases + 1)
        << "The remainder must propagate as capacity, not by taking a second donor release";

    fitting.reset();
    local.reset();
    oversized.reset();
}


/// A hierarchy turn is triggered by the release event, not limited by that event's byte count.
/// Another real decrease queued before the turn must be visible through aggregate accounting, so
/// two individually insufficient releases can satisfy one local request without extending the turn.
TEST(SchedulerSpaceShared, QueuedReleaseUsesAggregateCapacityFromSameTurn)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    inner_limit->basename = "inner";
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto first_donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    first_donor_queue->basename = "first_donor";
    AllocationQueue * first_donor_queue_ptr = first_donor_queue.get();
    inner_policy->attachChild(first_donor_queue);
    auto second_donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    second_donor_queue->basename = "second_donor";
    AllocationQueue * second_donor_queue_ptr = second_donor_queue.get();
    inner_policy->attachChild(second_donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    local_queue.reset();
    second_donor_queue.reset();
    first_donor_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    ManualAllocation first_donor(first_donor_queue_ptr, "first_donor", 4000);
    ManualAllocation second_donor(second_donor_queue_ptr, "second_donor", 4000);
    first_donor.offerUnusedCapacity(600);
    /// Publish the second decrease before the forced donor decrease reaches approval. Both ordinary
    /// decreases therefore precede the inner level's single queued handoff turn.
    first_donor.runOnNextReclaim([&] { second_donor.decreaseAsync(400); });

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the aggregate-release ordering gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(1000); // Initial work is the ordinary outer Fair winner.
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::atomic<bool> first_recorded{false};
    auto record_first = [&](const String & id)
    {
        if (!first_recorded.exchange(true))
            first_approval.set_value(id);
    };
    std::promise<void> release_local_approval;
    auto release_local_approval_future = release_local_approval.get_future().share();
    local->runOnNextApproval([&]
    {
        record_first("local");
        release_local_approval_future.wait();
    });
    external->runOnNextApproval([&] { record_first("external"); });
    start_scheduler.set_value();

    if (first_approval_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_local_approval.set_value();
        FAIL() << "Neither contender was approved after the two releases";
    }
    const String first = first_approval_future.get();
    EXPECT_EQ(first, "local")
        << "The inner level evaluated only the 600-byte event instead of actual 1000-byte availability";

    if (first == "local")
        t.scheduler.event_queue.enqueue([&] { local->removeAsync(); });
    else
        t.scheduler.event_queue.enqueue([&] { external->removeAsync(); });
    release_local_approval.set_value();

    if (first == "local")
    {
        ASSERT_TRUE(local->waitRemovedFor(std::chrono::seconds(5)));
        ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
        EXPECT_EQ(external->size(), 1000);
    }
    else
    {
        ASSERT_TRUE(external->waitRemovedFor(std::chrono::seconds(5)));
        t.scheduler.event_queue.enqueue([&] { local->removeAsync(); });
        ASSERT_TRUE(local->waitRemovedFor(std::chrono::seconds(5)));
    }

    EXPECT_EQ(first_donor.size(), 3400);
    EXPECT_EQ(second_donor.size(), 3600);
    EXPECT_EQ(first_donor.killCount(), 0u);
    EXPECT_EQ(second_donor.killCount(), 0u);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_EQ(external->killCount(), 0u);

    local.reset();
    external.reset();
}


/// Fitting search is a Fair optimization, not permission to cross a precedence boundary. A lower
/// precedence local request must remain blocked behind an oversized higher-precedence head, so the
/// exact release continues to the already-selected outer contender.
TEST(SchedulerSpaceShared, ReleasedCapacityDoesNotCrossPrecedenceBarrier)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    auto inner_policy = std::make_shared<PrecedenceAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_precedence";

    SchedulerNodeInfo high_info;
    high_info.setPrecedence(0);
    auto oversized_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, high_info);
    oversized_queue->basename = "oversized";
    AllocationQueue * oversized_queue_ptr = oversized_queue.get();
    inner_policy->attachChild(oversized_queue);

    SchedulerNodeInfo low_info;
    low_info.setPrecedence(1);
    auto fitting_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, low_info);
    fitting_queue->basename = "fitting";
    AllocationQueue * fitting_queue_ptr = fitting_queue.get();
    inner_policy->attachChild(fitting_queue);

    SchedulerNodeInfo donor_info;
    donor_info.setPrecedence(2);
    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, donor_info);
    donor_queue->basename = "donor";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    donor_queue.reset();
    fitting_queue.reset();
    oversized_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = donor_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-1000);
    donor.syncWithMemoryTracker(&donor_tracker);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the precedence handoff gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(1000);
    auto oversized = std::make_unique<ManualAllocation>(
        oversized_queue_ptr, "oversized", 2000, /* wait_for_admission = */ false);
    auto fitting = std::make_unique<ManualAllocation>(
        fitting_queue_ptr, "fitting", 1000, /* wait_for_admission = */ false);

    auto external_approved = std::make_shared<std::promise<void>>();
    auto external_approved_future = external_approved->get_future();
    std::promise<void> release_external_approval;
    auto release_external_approval_future = release_external_approval.get_future().share();
    external->runOnNextApproval([external_approved, release_external_approval_future]
    {
        external_approved->set_value();
        release_external_approval_future.wait();
    });
    start_scheduler.set_value();

    if (external_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_external_approval.set_value();
        FAIL() << "A lower-precedence fitting request captured capacity across the high-precedence barrier";
    }
    EXPECT_EQ(external->size(), 1000);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(oversized->size(), 0);
    EXPECT_EQ(fitting->size(), 0);

    oversized->removeAsync();
    fitting->removeAsync();
    release_external_approval.set_value();
    ASSERT_TRUE(oversized->waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(fitting->waitRemovedFor(std::chrono::seconds(5)));

    fitting.reset();
    oversized.reset();
    external.reset();
    donor_tracker.adjustWithUntrackedMemory(-7000);
}


/// A release can be committed before fitting local demand becomes visible. The exact decrease
/// remains offerable until its approval: a local request arriving in that interval receives the
/// subgraph's one selection, instead of letting the older outer contender consume the release.
TEST(SchedulerSpaceShared, LocalDemandArrivingBeforeReleaseApprovalGetsCredit)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    outer_limit->attachChild(policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    auto inner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_queue->basename = "inner_queue";
    AllocationQueue * inner_queue_ptr = inner_queue.get();
    inner_limit->attachChild(inner_queue);
    policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    inner_queue.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = inner_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-2000); // Reclaim only the exact deficit from 2000 reusable bytes.
    donor.syncWithMemoryTracker(&donor_tracker);

    auto retained = getSchedulerSnapshot(t, *inner_queue_ptr);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 8000);
    ASSERT_EQ(retained->queue_allocated, 8000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the late-local-demand gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(2000); // Needs exactly 1000 of the donor's retained capacity.

    /// Block the scheduler after the decrease has propagated to the root but before approval.
    /// At this point the inner graph had no local request, so the release is passing upward but
    /// remains tied to this exact approval cycle.
    std::promise<ResourceCost> reclaim_published;
    auto reclaim_published_future = reclaim_published.get_future();
    std::promise<void> release_reclaim_approval;
    auto release_reclaim_approval_future = release_reclaim_approval.get_future().share();
    std::promise<void> watcher_done;
    auto watcher_done_future = watcher_done.get_future();
    std::atomic<bool> stop_watcher{false};
    auto watcher = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weak_watcher = watcher;
    *watcher = [&, weak_watcher]
    {
        if (stop_watcher.load())
        {
            watcher_done.set_value();
            return;
        }
        if (t.scheduler.decrease && t.scheduler.decrease->allocation.id == "donor")
        {
            reclaim_published.set_value(t.scheduler.decrease->size);
            release_reclaim_approval_future.wait();
            watcher_done.set_value();
            return;
        }
        if (auto next = weak_watcher.lock())
            t.scheduler.event_queue.enqueue(EventQueue::Task(*next));
    };
    t.scheduler.event_queue.enqueue(EventQueue::Task(*watcher));
    start_scheduler.set_value();

    if (reclaim_published_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        stop_watcher = true;
        release_reclaim_approval.set_value();
        EXPECT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        FAIL() << "The exact retained-capacity decrease was not published before approval";
    }
    const ResourceCost published_reclaim = reclaim_published_future.get();
    if (published_reclaim != 1000)
    {
        release_reclaim_approval.set_value();
        EXPECT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        FAIL() << "Expected an exact 1000-byte reclaim, got " << published_reclaim;
    }

    auto local = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "local", 1000, /* wait_for_admission = */ false);
    std::promise<void> local_approved;
    auto local_approved_future = local_approved.get_future();
    std::promise<void> release_local_approval;
    auto release_local_approval_future = release_local_approval.get_future().share();
    local->runOnNextApproval([&]
    {
        local_approved.set_value();
        release_local_approval_future.wait();
    });

    /// This sentinel is queued after the local leaf activation. Event FIFO and the scheduler's
    /// drain-before-approve rule prove that local demand reached the hierarchy while the exact
    /// donor decrease was still awaiting approval.
    std::promise<void> local_published_before_approval;
    auto local_published_before_approval_future = local_published_before_approval.get_future();
    std::promise<void> continue_after_local_publication;
    auto continue_after_local_publication_future = continue_after_local_publication.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        local_published_before_approval.set_value();
        continue_after_local_publication_future.wait();
    });

    release_reclaim_approval.set_value();
    if (watcher_done_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        continue_after_local_publication.set_value();
        release_local_approval.set_value();
        FAIL() << "Scheduler did not leave the exact reclaim gate";
    }
    if (local_published_before_approval_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        continue_after_local_publication.set_value();
        release_local_approval.set_value();
        FAIL() << "Late local demand was not published before decrease approval";
    }
    continue_after_local_publication.set_value();
    if (local_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_local_approval.set_value();
        FAIL() << "Late fitting demand did not receive the committed local release";
    }
    EXPECT_EQ(local->size(), 1000);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_EQ(external->size(), 0);
    EXPECT_EQ(external->killCount(), 0u);

    /// The local request spent the first exact release. The outer request must then receive the
    /// donor's remaining unused kilobyte through a distinct ordinary reclaim round.
    release_local_approval.set_value();
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(external->size(), 2000);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    auto propagated = getSchedulerSnapshot(t, *inner_queue_ptr);
    ASSERT_TRUE(propagated.has_value());
    EXPECT_EQ(propagated->root_allocated, 9000);
    EXPECT_EQ(propagated->queue_allocated, 7000); // donor 6000 + local 1000
    EXPECT_EQ(propagated->queue_decreases, retained->queue_decreases + 2);
    EXPECT_FALSE(propagated->increase_pending);
    EXPECT_FALSE(propagated->decrease_pending);

    local.reset();
    external.reset();
    donor_tracker.adjustWithUntrackedMemory(-6000);
}


/// A release turn observes real aggregate capacity, not the size of one decrease. Existing free
/// capacity plus the new release may therefore make a larger local request fit; using it is not a
/// reservation or amplification, and does not require another donor release.
TEST(SchedulerSpaceShared, ReleasedCapacityUsesCurrentAggregateHeadroom)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    outer_limit->attachChild(policy);

    /// The local request needs the root's existing 1000 bytes plus the donor's 1000-byte release.
    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    auto inner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_queue->basename = "inner_queue";
    AllocationQueue * inner_queue_ptr = inner_queue.get();
    inner_limit->attachChild(inner_queue);
    policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    inner_queue.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = inner_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-1000);
    donor.syncWithMemoryTracker(&donor_tracker);

    auto retained = getSchedulerSnapshot(t, *inner_queue_ptr);
    ASSERT_TRUE(retained.has_value());
    ASSERT_EQ(retained->root_allocated, 8000);
    ASSERT_EQ(retained->queue_allocated, 8000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the aggregate-headroom ordering gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(2000); // Requires exactly 1000 released bytes at the outer limit.
    auto local = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "local", 2000, /* wait_for_admission = */ false);

    std::promise<void> local_approved;
    auto local_approved_future = local_approved.get_future();
    std::promise<void> release_local_approval;
    auto release_local_approval_future = release_local_approval.get_future().share();
    local->runOnNextApproval([&]
    {
        local_approved.set_value();
        release_local_approval_future.wait();
    });
    start_scheduler.set_value();

    if (local_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_local_approval.set_value();
        FAIL() << "The local request did not use the real post-release capacity";
    }
    EXPECT_EQ(local->size(), 2000);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_EQ(external->size(), 0);
    EXPECT_EQ(external->killCount(), 0u);

    /// Releasing the local allocation exposes the same actual capacity to ordinary outer policy.
    t.scheduler.event_queue.enqueue([&] { local->removeAsync(); });
    release_local_approval.set_value();
    ASSERT_TRUE(local->waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(external->size(), 2000);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    auto snapshot = getSchedulerSnapshot(t, *inner_queue_ptr);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 9000);
    EXPECT_EQ(snapshot->queue_allocated, 7000);
    EXPECT_EQ(snapshot->queue_decreases, retained->queue_decreases + 1);
    EXPECT_FALSE(snapshot->increase_pending);
    EXPECT_FALSE(snapshot->decrease_pending);

    local.reset();
    external.reset();
    donor_tracker.adjustWithUntrackedMemory(-7000);
}


/// A limit's Scheduled probe is not completed by an arbitrary smaller release already in flight.
/// Once that release makes both requests fit, ordinary Fair order—not a fabricated local
/// beneficiary—must choose the older Initial request first.
TEST(SchedulerSpaceShared, UnrelatedSmallerDecreaseDuringScheduledDoesNotCreateBeneficiary)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8500);
    inner_limit->basename = "inner";
    AllocationLimit * inner_limit_ptr = inner_limit.get();
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor_queue";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local_queue";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);
    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    local_queue.reset();
    donor_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    ManualAllocation donor(donor_queue_ptr, "donor", 8000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the unrelated-decrease ordering gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(1000); // Initial: normal Fair winner over the Pending local request.
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::atomic<bool> first_recorded{false};
    auto record_first = [&](const String & id)
    {
        if (!first_recorded.exchange(true))
            first_approval.set_value(id);
    };
    external->runOnNextApproval([&] { record_first("external"); });
    local->runOnNextApproval([&] { record_first("local"); });

    std::promise<std::pair<bool, ResourceCost>> decrease_observation;
    auto decrease_observation_future = decrease_observation.get_future();
    donor.runOnNextDecreaseApproval([&]
    {
        decrease_observation.set_value({inner_limit_ptr->hasUnusedCapacityReclaimPending(), local->size()});
    });

    /// This event runs after the local leaf has published its constrained request but before the
    /// limit's queued reclaim probe. Publish a direct 500-byte decrease synchronously, so approval
    /// observes Scheduled without allowing the probe to adopt this decrease as its exact result.
    t.scheduler.event_queue.enqueue([&]
    {
        donor.decreaseAsync(500);
        donor_queue_ptr->processActivation();
    });
    start_scheduler.set_value();

    if (decrease_observation_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        donor.runOnNextDecreaseApproval({});
        FAIL() << "The unrelated decrease was not approved";
    }
    const auto [reclaim_still_pending, local_size_at_decrease] = decrease_observation_future.get();
    EXPECT_TRUE(reclaim_still_pending) << "An unrelated decrease changed Scheduled into Beneficiary";
    EXPECT_EQ(local_size_at_decrease, 0);
    ASSERT_EQ(first_approval_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(first_approval_future.get(), "external");
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(local->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(donor.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(donor.size(), 7500);
    EXPECT_EQ(donor.killCount(), 0u);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(local->killCount(), 0u);

    local.reset();
    external.reset();
}


/// Once an exact release has produced a beneficiary, cancelling that exact request spends the
/// handoff. A larger replacement cannot inherit one kilobyte of credit and leap ahead of the
/// older outer Initial request.
TEST(SchedulerSpaceShared, CancelledBeneficiaryDoesNotTransferCreditToLargerReplacement)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    AllocationLimit * inner_limit_ptr = inner_limit.get();
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor_queue";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local_queue";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);
    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    local_queue.reset();
    donor_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    ManualAllocation donor(donor_queue_ptr, "donor", 8000);
    donor.offerUnusedCapacity(1000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the beneficiary-replacement gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(2000);
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<bool> exact_release_approved;
    auto exact_release_approved_future = exact_release_approved.get_future();
    std::promise<void> continue_after_exact_release;
    auto continue_after_exact_release_future = continue_after_exact_release.get_future().share();
    donor.runOnNextDecreaseApproval([&]
    {
        exact_release_approved.set_value(inner_limit_ptr->hasUnusedCapacityReclaimPending());
        continue_after_exact_release_future.wait();
    });

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::promise<void> release_first_approval;
    auto release_first_approval_future = release_first_approval.get_future().share();
    external->runOnNextApproval([&]
    {
        first_approval.set_value("external");
        release_first_approval_future.wait();
    });

    /// The outer probe asks the donor for its exact 1000 reusable bytes after both leaf requests
    /// are visible. The inner graph assigns that scheduler-selected handoff to `local`; ordinary
    /// decrease approval then moves it to Beneficiary before the callback gate opens.
    start_scheduler.set_value();
    if (exact_release_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        donor.runOnNextDecreaseApproval({});
        continue_after_exact_release.set_value();
        release_first_approval.set_value();
        FAIL() << "The exact donor decrease was not approved";
    }
    const bool exact_reclaim_still_pending = exact_release_approved_future.get();
    if (exact_reclaim_still_pending)
    {
        continue_after_exact_release.set_value();
        release_first_approval.set_value();
        FAIL() << "The exact release did not reach Beneficiary before cancellation";
    }

    std::unique_ptr<ManualAllocation> replacement;
    std::promise<void> replacement_created;
    auto replacement_created_future = replacement_created.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        local->removeAsync();
        replacement = std::make_unique<ManualAllocation>(
            local_queue_ptr, "replacement", 2000, /* wait_for_admission = */ false);
        replacement->runOnNextApproval([&]
        {
            first_approval.set_value("replacement");
            release_first_approval_future.wait();
        });
        replacement_created.set_value();
    });
    continue_after_exact_release.set_value();

    if (replacement_created_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_first_approval.set_value();
        FAIL() << "Replacement request was not created after beneficiary cancellation";
    }
    if (!local->waitRemovedFor(std::chrono::seconds(5)))
    {
        release_first_approval.set_value();
        FAIL() << "Cancelled beneficiary was not removed before the next approval";
    }
    if (first_approval_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_first_approval.set_value();
        FAIL() << "Neither normal contender nor replacement was approved";
    }
    const String first = first_approval_future.get();
    EXPECT_EQ(first, "external");

    if (first == "external")
        t.scheduler.event_queue.enqueue([&] { replacement->removeAsync(); });
    else
        t.scheduler.event_queue.enqueue([&] { external->removeAsync(); });
    release_first_approval.set_value();

    if (first == "external")
    {
        ASSERT_TRUE(replacement->waitRemovedFor(std::chrono::seconds(5)));
        EXPECT_EQ(external->size(), 2000);
    }
    else
    {
        ASSERT_TRUE(external->waitRemovedFor(std::chrono::seconds(5)));
        EXPECT_EQ(replacement->size(), 2000);
    }
    ASSERT_TRUE(donor.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(donor.size(), 7000);
    EXPECT_EQ(donor.killCount(), 0u);

    replacement.reset();
    local.reset();
    external.reset();
}


/// Cancellation can race an exact reclaim before the donor decrease is approved. The request
/// object may be destroyed in that interval: the committed decrease remains ordinary capacity,
/// while its old InFlight pointer and one-selection handoff must disappear with the claimant.
TEST(SchedulerSpaceShared, DestroyedInFlightClaimantCannotTransferCredit)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    inner_limit->basename = "inner";
    AllocationLimit * inner_limit_ptr = inner_limit.get();
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor_queue";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local_queue";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);
    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    local_queue.reset();
    donor_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    ManualAllocation donor(donor_queue_ptr, "donor", 8000);
    donor.offerUnusedCapacity(1000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the InFlight-cancellation gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(2000);
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::promise<void> release_first_approval;
    auto release_first_approval_future = release_first_approval.get_future().share();
    std::atomic<bool> first_recorded{false};
    auto record_first = [&](const String & id)
    {
        if (!first_recorded.exchange(true))
        {
            first_approval.set_value(id);
            release_first_approval_future.wait();
        }
    };
    external->runOnNextApproval([&] { record_first("external"); });

    /// Event processing precedes scheduler approvals. Requeue this watcher until the exact donor
    /// decrease has reached the root, then hold that approval so cancellation can be placed ahead
    /// of it without a timing delay.
    std::promise<std::pair<ResourceCost, bool>> reclaim_published;
    auto reclaim_published_future = reclaim_published.get_future();
    std::promise<void> release_reclaim_approval;
    auto release_reclaim_approval_future = release_reclaim_approval.get_future().share();
    std::promise<void> watcher_done;
    auto watcher_done_future = watcher_done.get_future();
    std::atomic<bool> stop_watcher{false};
    auto watcher = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weak_watcher = watcher;
    *watcher = [&, weak_watcher]
    {
        if (stop_watcher.load())
        {
            watcher_done.set_value();
            return;
        }
        if (t.scheduler.decrease && t.scheduler.decrease->allocation.id == "donor")
        {
            reclaim_published.set_value(
                {t.scheduler.decrease->size, inner_limit_ptr->hasUnusedCapacityReclaimPending()});
            release_reclaim_approval_future.wait();
            watcher_done.set_value();
            return;
        }
        if (auto next = weak_watcher.lock())
            t.scheduler.event_queue.enqueue(EventQueue::Task(*next));
    };
    t.scheduler.event_queue.enqueue(EventQueue::Task(*watcher));
    start_scheduler.set_value();

    if (reclaim_published_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        stop_watcher = true;
        release_reclaim_approval.set_value();
        release_first_approval.set_value();
        EXPECT_EQ(watcher_done_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        FAIL() << "The exact donor decrease never reached the global approval gate";
    }
    const auto [reclaimed_size, reclaim_in_flight] = reclaim_published_future.get();
    EXPECT_EQ(reclaimed_size, 1000);
    EXPECT_TRUE(reclaim_in_flight);

    /// Cancel the pending claimant through its normal queued leaf activation, then stop the
    /// scheduler at a FIFO sentinel behind that activation. The test thread can now destroy the
    /// request object before the donor decrease approval touches any retained state, without
    /// bypassing the event queue's activation-id bookkeeping.
    std::promise<void> cancellation_published;
    auto cancellation_published_future = cancellation_published.get_future();
    std::promise<void> continue_after_destroy;
    auto continue_after_destroy_future = continue_after_destroy.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        local->removeAsync();
        t.scheduler.event_queue.enqueue([&]
        {
            cancellation_published.set_value();
            continue_after_destroy_future.wait();
        });
    });
    release_reclaim_approval.set_value();

    if (watcher_done_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready
        || cancellation_published_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        continue_after_destroy.set_value();
        release_first_approval.set_value();
        FAIL() << "The exact claimant was not cancelled before decrease approval";
    }
    if (!local->waitRemovedFor(std::chrono::seconds(5)))
    {
        continue_after_destroy.set_value();
        release_first_approval.set_value();
        FAIL() << "The cancelled InFlight claimant remained attached";
    }
    local.reset(); // Any stale raw reclaim pointer is now immediately invalid under ASan.

    auto replacement = std::make_unique<ManualAllocation>(
        local_queue_ptr, "replacement", 2000, /* wait_for_admission = */ false);
    replacement->runOnNextApproval([&] { record_first("replacement"); });
    continue_after_destroy.set_value();

    if (first_approval_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_first_approval.set_value();
        t.scheduler.event_queue.enqueue([&]
        {
            replacement->removeAsync();
            external->removeAsync();
        });
        EXPECT_TRUE(replacement->waitRemovedFor(std::chrono::seconds(5)));
        EXPECT_TRUE(external->waitRemovedFor(std::chrono::seconds(5)));
        FAIL() << "The committed release did not return to normal policy after cancellation";
    }
    const String first = first_approval_future.get();
    EXPECT_EQ(first, "external");

    if (first == "external")
        t.scheduler.event_queue.enqueue([&] { replacement->removeAsync(); });
    else
        t.scheduler.event_queue.enqueue([&] { external->removeAsync(); });
    release_first_approval.set_value();

    if (first == "external")
    {
        ASSERT_TRUE(replacement->waitRemovedFor(std::chrono::seconds(5)));
        ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
        EXPECT_EQ(external->size(), 2000);
    }
    else
    {
        ASSERT_TRUE(external->waitRemovedFor(std::chrono::seconds(5)));
        ASSERT_TRUE(replacement->waitSyncedFor(std::chrono::seconds(5)));
        EXPECT_EQ(replacement->size(), 2000);
    }
    ASSERT_TRUE(donor.waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(donor.size(), 7000);
    EXPECT_EQ(donor.killCount(), 0u);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(replacement->killCount(), 0u);

    replacement.reset();
    external.reset();
}


/// Removing an admitted zero-byte allocation is topology cleanup, not a release. While a reclaim
/// probe is Scheduled, that zero decrease must neither complete the probe nor grant its request a
/// beneficiary that can be transferred to a later fitting replacement.
TEST(SchedulerSpaceShared, ZeroByteRemovalCannotSatisfyScheduledReclaim)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto outer_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_policy->basename = "outer_fair";
    outer_limit->attachChild(outer_policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8500);
    inner_limit->basename = "inner";
    AllocationLimit * inner_limit_ptr = inner_limit.get();
    auto inner_policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_policy->basename = "inner_fair";
    inner_limit->attachChild(inner_policy);
    outer_policy->attachChild(inner_limit);

    auto donor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    donor_queue->basename = "donor_queue";
    AllocationQueue * donor_queue_ptr = donor_queue.get();
    inner_policy->attachChild(donor_queue);
    auto local_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    local_queue->basename = "local_queue";
    AllocationQueue * local_queue_ptr = local_queue.get();
    inner_policy->attachChild(local_queue);
    auto zero_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    zero_queue->basename = "zero_queue";
    AllocationQueue * zero_queue_ptr = zero_queue.get();
    inner_policy->attachChild(zero_queue);
    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    outer_policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    zero_queue.reset();
    local_queue.reset();
    donor_queue.reset();
    inner_policy.reset();
    inner_limit.reset();
    outer_policy.reset();
    outer_limit.reset();
    r.registerResource();

    ManualAllocation donor(donor_queue_ptr, "donor", 8000);
    auto zero = std::make_unique<ManualAllocation>(zero_queue_ptr, "zero", 1);
    zero->decreaseAsync(1); // Keep it admitted while reducing its allocation to zero.
    ASSERT_TRUE(zero->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_EQ(zero->size(), 0);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the zero-removal ordering gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(500);
    auto local = std::make_unique<ManualAllocation>(
        local_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<std::pair<bool, ResourceCost>> zero_removal_approved;
    auto zero_removal_approved_future = zero_removal_approved.get_future();
    std::promise<void> continue_after_zero_removal;
    auto continue_after_zero_removal_future = continue_after_zero_removal.get_future().share();
    zero->runOnNextDecreaseApproval([&]
    {
        zero_removal_approved.set_value({inner_limit_ptr->hasUnusedCapacityReclaimPending(), local->size()});
        continue_after_zero_removal_future.wait();
    });

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::atomic<bool> first_recorded{false};
    auto record_first = [&](const String & id)
    {
        if (!first_recorded.exchange(true))
            first_approval.set_value(id);
    };
    external->runOnNextApproval([&] { record_first("external"); });

    /// Publish the zero-byte removal after the local request has made the inner limit Scheduled,
    /// but before its queued probe can run.
    t.scheduler.event_queue.enqueue([&]
    {
        zero->removeAsync();
        zero_queue_ptr->processActivation();
    });
    start_scheduler.set_value();
    if (zero_removal_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        zero->runOnNextDecreaseApproval({});
        continue_after_zero_removal.set_value();
        FAIL() << "The admitted zero-byte allocation was not removed";
    }
    const auto [reclaim_pending_after_zero, local_size_after_zero] = zero_removal_approved_future.get();
    if (!reclaim_pending_after_zero || local_size_after_zero != 0)
    {
        continue_after_zero_removal.set_value();
        FAIL() << "Zero-byte topology cleanup satisfied a Scheduled reclaim";
    }

    std::unique_ptr<ManualAllocation> replacement;
    std::promise<void> replacement_created;
    auto replacement_created_future = replacement_created.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        local->removeAsync();
        replacement = std::make_unique<ManualAllocation>(
            local_queue_ptr, "replacement", 500, /* wait_for_admission = */ false);
        replacement->runOnNextApproval([&] { record_first("replacement"); });
        replacement_created.set_value();
    });
    continue_after_zero_removal.set_value();

    if (replacement_created_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        FAIL() << "Replacement request was not created after zero-byte removal";
    ASSERT_TRUE(zero->waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(local->waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_EQ(first_approval_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(first_approval_future.get(), "external");
    ASSERT_TRUE(external->waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(replacement->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(donor.killCount(), 0u);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(replacement->killCount(), 0u);

    replacement.reset();
    local.reset();
    external.reset();
    zero.reset();
}


/// Local-credit handoff is subordinate to explicit workload precedence. If the external request
/// belongs to a strictly higher-precedence branch, it must receive the newly released capacity
/// before the lower-precedence local beneficiary; equal-precedence locality is tested above.
TEST(SchedulerSpaceShared, HigherPrecedenceOuterContenderOverridesLowerLocalCredit)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto policy = std::make_shared<PrecedenceAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "precedence";
    outer_limit->attachChild(policy);

    SchedulerNodeInfo low_info;
    low_info.setPrecedence(1);
    /// The local +1000 request fits this limit; precedence alone decides who receives the handoff.
    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, low_info, 9000);
    inner_limit->basename = "low";
    auto inner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_queue->basename = "inner_queue";
    AllocationQueue * inner_queue_ptr = inner_queue.get();
    inner_limit->attachChild(inner_queue);
    policy->attachChild(inner_limit);

    SchedulerNodeInfo high_info;
    high_info.setPrecedence(0);
    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, high_info);
    external_queue->basename = "high";
    AllocationQueue * external_queue_ptr = external_queue.get();
    policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    inner_queue.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = inner_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-1000);
    donor.syncWithMemoryTracker(&donor_tracker);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the precedence ordering gate";
    }

    /// Enqueue the high request first so the outer probe is scheduled before the inner probe. The
    /// outer probe waits for that inner reclaim round, but once the decrease lands, precedence—not
    /// the lower branch's beneficiary marker—must choose the next request.
    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(2000);
    auto local = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<void> external_approved;
    auto external_approved_future = external_approved.get_future();
    std::promise<void> release_external_approval;
    auto release_external_approval_future = release_external_approval.get_future().share();
    external->runOnNextApproval([&]
    {
        external_approved.set_value();
        release_external_approval_future.wait();
    });
    start_scheduler.set_value();

    if (external_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_external_approval.set_value();
        FAIL() << "Lower-precedence local credit blocked the higher-precedence contender";
    }
    EXPECT_EQ(external->size(), 2000);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(local->size(), 0);
    EXPECT_EQ(local->killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    release_external_approval.set_value();
    local.reset();
    external.reset();
    donor_tracker.adjustWithUntrackedMemory(-7000);
}


/// Fitting demand is not itself credit. With no positive unused-capacity decrease to hand off, the
/// inner request keeps its ordinary Fair position: the older `Initial` contender reaches eviction
/// and is approved first after the victim releases. This prevents speculative local reservations.
TEST(SchedulerSpaceShared, LocalDemandWithoutReleasedCapacityGetsNoCredit)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    outer_limit->attachChild(policy);

    /// The local request is fitting, but there is deliberately no released capacity to offer it.
    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 9000);
    inner_limit->basename = "inner";
    auto inner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_queue->basename = "inner_queue";
    AllocationQueue * inner_queue_ptr = inner_queue.get();
    inner_limit->attachChild(inner_queue);
    policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    inner_queue.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    /// This allocation offers no reusable capacity: all 8000 charged bytes are busy.
    auto donor = std::make_unique<ManualAllocation>(inner_queue_ptr, "donor", 8000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the no-credit ordering gate";
    }

    auto external = std::make_unique<ManualAllocation>(external_queue_ptr, "external", 0);
    external->increaseAsync(2000);
    auto local = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    std::promise<void> external_approved;
    auto external_approved_future = external_approved.get_future();
    std::promise<void> release_external_approval;
    auto release_external_approval_future = release_external_approval.get_future().share();
    external->runOnNextApproval([&]
    {
        external_approved.set_value();
        release_external_approval_future.wait();
    });
    start_scheduler.set_value();

    ASSERT_TRUE(donor->waitKillsFor(1, std::chrono::seconds(5)))
        << "Normal eviction was bypassed even though no capacity was released";
    EXPECT_EQ(local->size(), 0);
    EXPECT_EQ(local->killCount(), 0u);

    donor.reset(); // Complete the selected eviction and make the ordinary Fair order observable.
    if (external_approved_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_external_approval.set_value();
        FAIL() << "Fitting local demand manufactured credit ahead of the older Initial request";
    }
    EXPECT_EQ(external->size(), 2000);
    EXPECT_EQ(external->killCount(), 0u);
    EXPECT_EQ(local->size(), 0);
    EXPECT_EQ(local->killCount(), 0u);

    release_external_approval.set_value();
    ASSERT_TRUE(local->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(local->size(), 1000);
    EXPECT_EQ(local->killCount(), 0u);

    auto snapshot = getSchedulerSnapshot(t, *inner_queue_ptr);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 3000);
    EXPECT_EQ(snapshot->queue_allocated, 1000);
    EXPECT_FALSE(snapshot->increase_pending);
    EXPECT_FALSE(snapshot->decrease_pending);

    local.reset();
    external.reset();
}


/// An outer reclaim probe may already have yielded to an inner scheduled probe when the inner
/// request disappears. Cancelling that request is an explicit completion of the inner round: the
/// outer probe must be woken and allowed to claim the now-uncontended slack without polling.
TEST(SchedulerSpaceShared, CancelledInnerReclaimProbeWakesOuter)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "fair";
    outer_limit->attachChild(policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 8000);
    inner_limit->basename = "inner";
    auto inner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_queue->basename = "inner_queue";
    AllocationQueue * inner_queue_ptr = inner_queue.get();
    inner_limit->attachChild(inner_queue);
    policy->attachChild(inner_limit);

    auto external_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    external_queue->basename = "external";
    AllocationQueue * external_queue_ptr = external_queue.get();
    policy->attachChild(external_queue);

    r.root_node = outer_limit;
    external_queue.reset();
    inner_queue.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    ResourceLink donor_link;
    donor_link.allocation_queue = inner_queue_ptr;
    MemoryTracker donor_tracker;
    MemoryReservation donor(donor_link, "donor", 0);
    donor_tracker.adjustWithUntrackedMemory(8000);
    donor.syncWithMemoryTracker(&donor_tracker);
    donor_tracker.adjustWithUntrackedMemory(-1000);
    donor.syncWithMemoryTracker(&donor_tracker);

    ManualAllocation external(external_queue_ptr, "external", 0);

    std::promise<void> setup_gate_entered;
    std::promise<void> release_setup_gate;
    auto release_setup_gate_future = release_setup_gate.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        setup_gate_entered.set_value();
        release_setup_gate_future.wait();
    });
    if (setup_gate_entered.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_setup_gate.set_value();
        FAIL() << "Scheduler did not reach the deterministic setup gate";
    }

    external.increaseAsync(3000); // Outer deficit is exactly the donor's unused 1000 bytes.

    /// Event ids enforce this order without sleeps:
    /// external queue -> blocker -> inner queue -> outer probe -> cancellation -> inner probe.
    /// The outer probe therefore observes and waits for the scheduled inner probe before the
    /// cancellation is applied.
    std::promise<void> blocker_entered;
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        blocker_entered.set_value();
        release_blocker_future.wait();
    });

    auto local = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "local", 1000, /* wait_for_admission = */ false);

    release_setup_gate.set_value();

    if (blocker_entered.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_blocker.set_value();
        FAIL() << "Scheduler did not reach the cancellation ordering gate";
    }

    std::promise<void> cancellation_processed;
    auto cancellation_processed_future = cancellation_processed.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        inner_queue_ptr->removeAllocation(*local);
        /// Process the queued removal at this exact scheduler-thread position. The regular queue
        /// activation remains queued and becomes a harmless no-op; invoking the public activation
        /// hook here is solely what places cancellation between the outer and inner probes.
        inner_queue_ptr->processActivation();
        cancellation_processed.set_value();
    });
    release_blocker.set_value();

    ASSERT_EQ(cancellation_processed_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_TRUE(external.waitSyncedFor(std::chrono::seconds(5)))
        << "The outer probe was not woken after the scheduled inner round was cancelled";
    EXPECT_EQ(external.size(), 3000);
    EXPECT_EQ(external.killCount(), 0u);
    EXPECT_NO_THROW(donor.syncWithMemoryTracker(&donor_tracker));

    auto snapshot = getSchedulerSnapshot(t, *inner_queue_ptr);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 10000);
    EXPECT_EQ(snapshot->queue_allocated, 7000);
    EXPECT_FALSE(snapshot->increase_pending);
    EXPECT_FALSE(snapshot->decrease_pending);

    donor_tracker.adjustWithUntrackedMemory(-7000);
}


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


/// A running query asking for an increase that cannot fit must not hide smaller new reservations that
/// still fit in the remaining budget. The large growth stays parked while the beneficiaries run and is
/// reconsidered only after they finish. This is suspension before eviction, not just admission reordering.
TEST(SchedulerSpaceShared, PendingAllocationsRunWhileBlockedGrowthIsSuspended)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    /// Park the scheduler so both requests are visible in the queue at once. `AllocationQueue` normally
    /// presents running-query increases before pending new allocations, so `heavy` is head-of-line.
    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000); // Cannot fit: 8000 + 5000 > 10000.
    auto small = std::make_unique<ManualAllocation>(queue, "small", 1000, /* wait_for_admission = */ false);
    auto next_small = std::make_unique<ManualAllocation>(queue, "next_small", 1000, /* wait_for_admission = */ false);
    release.set_value();

    /// Admission proves both smaller reservations bypassed the blocked growth. They remain beneficiaries
    /// until removal, so merely admitting them must not cause `heavy` to be killed.
    small->waitSynced();
    next_small->waitSynced();
    EXPECT_EQ(heavy.size(), 8000);
    EXPECT_EQ(small->size(), 1000);
    EXPECT_EQ(next_small->size(), 1000);
    EXPECT_EQ(heavy.killCount(), 0u);

    /// Finishing one beneficiary is not enough while another is still making progress.
    small.reset();
    EXPECT_EQ(heavy.killCount(), 0u);

    /// Once the last beneficiary finishes, the blocked +5000 growth is reconsidered. It still cannot fit
    /// with `heavy` holding 8000, so suspension has exhausted its useful work and the existing kill path runs.
    next_small.reset();
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
}


/// Suspension does not serialize work unnecessarily. Any memory release is a progress checkpoint: if it
/// creates enough headroom for the parked growth, that growth resumes while its beneficiary is still alive.
TEST(SchedulerSpaceShared, MemoryReleaseLetsSuspendedGrowthResume)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    auto releaser = std::make_unique<ManualAllocation>(queue, "releaser", 3000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(2000); // 9000 + 2000 > 10000, so growth is suspended.
    auto small = std::make_unique<ManualAllocation>(queue, "small", 500, /* wait_for_admission = */ false);
    release.set_value();

    small->waitSynced(); // Total is now 9500; `heavy` remains parked and `small` keeps running.
    EXPECT_EQ(heavy.killCount(), 0u);

    releaser->decreaseAsync(2000); // Total becomes 7500, enough to satisfy `heavy`'s parked +2000.
    releaser->waitSynced();
    heavy.waitSynced();

    EXPECT_EQ(heavy.killCount(), 0u);
    EXPECT_EQ(heavy.size(), 8000);
    EXPECT_EQ(releaser->size(), 1000);
    EXPECT_EQ(small->size(), 500);
}


/// If the work admitted ahead of suspended growth later needs memory that cannot fit, suspension has not
/// restored flow. The existing eviction policy may then kill the suspended heavy allocation so the winner
/// can continue; killing remains a last resort reached by resource state, not by elapsed wait time.
TEST(SchedulerSpaceShared, BeneficiaryBlockedOnGrowthCanEvictSuspendedAllocation)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.protectAfterPressureRounds(2);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto small = std::make_unique<ManualAllocation>(queue, "small", 1000, /* wait_for_admission = */ false);
    release.set_value();

    small->waitSynced();
    EXPECT_EQ(heavy.killCount(), 0u);

    small->increaseAsync(2000); // 8000 + 1000 + 2000 > 10000: the winner itself can no longer progress.
    heavy.recoveryCheckpoint(); // External reclaim controller reports that its protected pass made no room.
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
}


/// If even the first alternative request cannot fit, there is no beneficiary that can make progress.
/// The suspended growth therefore falls through to the existing eviction path immediately.
TEST(SchedulerSpaceShared, BlockedAlternativeFallsBackToEviction)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto blocked = std::make_unique<ManualAllocation>(queue, "blocked", 3000, /* wait_for_admission = */ false);
    release.set_value();

    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
    EXPECT_EQ(blocked->size(), 0);
}


/// A non-fitting alternative must not terminate the search. The queue preserves FIFO order for normal
/// admission, but within a suspension round every later request gets a fit check before eviction.
TEST(SchedulerSpaceShared, FittingAllocationBehindBlockedAlternativeRunsBeforeEviction)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto blocked = std::make_unique<ManualAllocation>(queue, "blocked", 3000, /* wait_for_admission = */ false);
    auto fitting = std::make_unique<ManualAllocation>(queue, "fitting", 1000, /* wait_for_admission = */ false);
    release.set_value();

    fitting->waitSynced();
    EXPECT_EQ(fitting->size(), 1000);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);

    /// Releasing the fitting beneficiary starts a new resource-state round. The +3000 request is
    /// reconsidered, still cannot fit, and only then may the original growth reach eviction.
    fitting.reset();
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
}


template <typename Policy>
void suspendedIncreaseIsHiddenThroughPolicyHierarchy()
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<Policy>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    SchedulerNodeInfo heavy_info;
    heavy_info.setPrecedence(0);
    auto heavy_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, heavy_info);
    heavy_queue->basename = "heavy_queue";
    AllocationQueue * heavy_queue_ptr = heavy_queue.get();
    policy->attachChild(heavy_queue);

    SchedulerNodeInfo small_info;
    small_info.setPrecedence(1);
    auto small_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, small_info);
    small_queue->basename = "small_queue";
    AllocationQueue * small_queue_ptr = small_queue.get();
    policy->attachChild(small_queue);

    r.root_node = limit;
    /// The holder must own the complete subtree so destruction happens on the scheduler thread.
    small_queue.reset();
    heavy_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(heavy_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    /// The sibling activation is queued before the heavy queue's post-suspension activation. Parent
    /// policies must nevertheless stop selecting the stale +5000 request as soon as it is suspended.
    heavy.increaseAsync(5000);
    auto small = std::make_unique<ManualAllocation>(small_queue_ptr, "small", 1000, /* wait_for_admission = */ false);

    release.set_value();

    /// Events are processed before approvals, so observe the policy through the actual admission
    /// completion rather than an event that can legitimately run first.
    small->waitSynced();
    EXPECT_EQ(small->size(), 1000);
    EXPECT_EQ(heavy.killCount(), 0u);

    /// The admitted sibling remains productive work, so the heavy query stays suspended until the
    /// sibling releases its allocation. That release retries the growth across queue boundaries.
    EXPECT_EQ(heavy.killCount(), 0u);
    small.reset();
    heavy.waitKills(1);
}


TEST(SchedulerSpaceShared, SuspendedIncreaseIsHiddenThroughFairHierarchy)
{
    suspendedIncreaseIsHiddenThroughPolicyHierarchy<FairAllocation>();
}


TEST(SchedulerSpaceShared, SuspendedIncreaseIsHiddenThroughPrecedenceHierarchy)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<PrecedenceAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    SchedulerNodeInfo high_info;
    high_info.setPrecedence(0);
    auto high_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, high_info);
    high_queue->basename = "high";
    AllocationQueue * high_queue_ptr = high_queue.get();
    policy->attachChild(high_queue);

    SchedulerNodeInfo low_info;
    low_info.setPrecedence(1);
    auto low_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, low_info);
    low_queue->basename = "low";
    AllocationQueue * low_queue_ptr = low_queue.get();
    policy->attachChild(low_queue);

    r.root_node = limit;
    low_queue.reset();
    high_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(high_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto lower_precedence = std::make_unique<ManualAllocation>(
        low_queue_ptr, "lower_precedence", 1000, /* wait_for_admission = */ false);
    release.set_value();

    /// Suspension does not override workload precedence. The high-precedence request reaches its
    /// existing last resort before lower-precedence work can be admitted merely because it fits.
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
    EXPECT_EQ(lower_precedence->size(), 0);
}


TEST(SchedulerSpaceShared, FittingRegularGrowthRemainsProductive)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    auto other = std::make_unique<ManualAllocation>(queue, "other", 1000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto anchor = std::make_unique<ManualAllocation>(queue, "anchor", 500, /* wait_for_admission = */ false);
    release.set_value();

    anchor->waitSynced();
    other->increaseAsync(500);
    other->waitSynced();
    EXPECT_EQ(other->size(), 1500);
    EXPECT_EQ(heavy.killCount(), 0u);

    /// The anchor can finish without ending the suspension while the regular-growth winner is active.
    anchor.reset();
    EXPECT_EQ(heavy.killCount(), 0u);

    other.reset();
    heavy.waitKills(1);
}


/// Pending demand is not a reservation. A regular increase that fits the current hard limit must
/// complete before a later pending admission; only a real failed fit may open a suspension round
/// that protects capacity for competing work.
TEST(SchedulerSpaceShared, FittingRegularGrowthPrecedesHypotheticalPendingDemand)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    ManualAllocation releaser(queue, "releaser", 3000);

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the steady-state ordering gate";
    }

    heavy.increaseAsync(1000); // Fits exactly: 6000 + 3000 + 1000 == 10000.
    auto small = std::make_unique<ManualAllocation>(
        queue, "small", 500, /* wait_for_admission = */ false);

    std::promise<String> first_approval;
    auto first_approval_future = first_approval.get_future();
    std::promise<void> release_first_approval;
    auto release_first_approval_future = release_first_approval.get_future().share();
    std::atomic<bool> first_recorded{false};
    auto record_first = [&](const String & id)
    {
        if (!first_recorded.exchange(true))
        {
            first_approval.set_value(id);
            release_first_approval_future.wait();
        }
    };
    heavy.runOnNextApproval([&] { record_first("heavy"); });
    small->runOnNextApproval([&] { record_first("small"); });
    start_scheduler.set_value();

    if (first_approval_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        /// Make teardown possible even if the ordering path is broken before either callback.
        t.scheduler.event_queue.enqueue([&] { releaser.decreaseAsync(1000); });
        release_first_approval.set_value();
        FAIL() << "Neither fitting request was approved";
    }
    EXPECT_EQ(first_approval_future.get(), "heavy");
    EXPECT_EQ(heavy.size(), 7000);
    EXPECT_EQ(small->size(), 0);
    EXPECT_EQ(heavy.pressureCount(), 0u)
        << "A fitting regular increase opened a hypothetical suspension round";

    /// The first callback holds the queue mutex, so enqueue the real release on the scheduler
    /// thread. It happens only after the fitting growth has committed and makes room for `small`.
    t.scheduler.event_queue.enqueue([&] { releaser.decreaseAsync(500); });
    release_first_approval.set_value();

    ASSERT_TRUE(releaser.waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(heavy.waitSyncedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(small->waitSyncedFor(std::chrono::seconds(5)));
    EXPECT_EQ(heavy.size(), 7000);
    EXPECT_EQ(releaser.size(), 2500);
    EXPECT_EQ(small->size(), 500);
    EXPECT_EQ(heavy.killCount(), 0u);
    EXPECT_EQ(releaser.killCount(), 0u);
    EXPECT_EQ(small->killCount(), 0u);
}


/// The queue keeps one policy/eviction owner, but each blocked reservation is a distinct dependency
/// graph and must retain its own recovery lane. An immediate completion from the second graph is
/// reconciled on that exact allocation without replacing or killing the first policy owner.
TEST(SchedulerSpaceShared, EachBlockedReservationKeepsItsOwnRecoveryLane)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation owner(queue, "owner", 6000);
    ManualAllocation follower(queue, "follower", 3000);
    owner.protectAfterPressureRounds(100); // Keep the first owner episode open for observation.
    follower.protectAfterPressureRounds(100);
    follower.reconcilePendingIncreaseTo(0);
    follower.runOnNextPressure([&] { follower.recoveryCheckpoint(); });

    std::promise<void> scheduler_parked;
    std::promise<void> start_scheduler;
    auto start_scheduler_future = start_scheduler.get_future().share();
    t.scheduler.event_queue.enqueue([&]
    {
        scheduler_parked.set_value();
        start_scheduler_future.wait();
    });
    if (scheduler_parked.get_future().wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        start_scheduler.set_value();
        FAIL() << "Scheduler did not reach the per-allocation recovery gate";
    }

    owner.increaseAsync(5000);    // Fair key 11000: becomes this queue's recovery owner.
    follower.increaseAsync(9000); // Fair key 12000: gets its own recovery lane behind the owner.
    auto fitting = std::make_unique<ManualAllocation>(
        queue, "fitting", 500, /* wait_for_admission = */ false);
    start_scheduler.set_value();

    ASSERT_TRUE(fitting->waitSyncedFor(std::chrono::seconds(5)))
        << "The second blocked growth hid fitting work instead of yielding within the owner episode";
    ASSERT_TRUE(follower.waitSyncedFor(std::chrono::seconds(5)))
        << "The second dependency graph's immediate recovery completion was not reconciled";
    EXPECT_EQ(fitting->size(), 500);
    EXPECT_GE(owner.pressureCount(), 1u);
    EXPECT_EQ(follower.pressureCount(), 1u)
        << "A non-owner dependency graph lost its own recovery lane";
    EXPECT_EQ(owner.size(), 6000);
    EXPECT_EQ(follower.size(), 3000);
    EXPECT_EQ(owner.killCount(), 0u);
    EXPECT_EQ(follower.killCount(), 0u);

    owner.removeAsync();
    follower.removeAsync();
    ASSERT_TRUE(owner.waitRemovedFor(std::chrono::seconds(5)));
    ASSERT_TRUE(follower.waitRemovedFor(std::chrono::seconds(5)));
}


TEST(SchedulerSpaceShared, BeneficiaryReleasingToZeroEndsSuspension)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    ManualAllocation beneficiary(queue, "beneficiary", 1000, /* wait_for_admission = */ false);
    release.set_value();

    beneficiary.waitSynced();
    EXPECT_EQ(heavy.killCount(), 0u);

    /// Releasing all memory ends productive membership even though the allocation object stays alive.
    beneficiary.decreaseAsync(1000);
    beneficiary.waitSynced();
    EXPECT_EQ(beneficiary.size(), 0);
    heavy.waitKills(1);
}


TEST(SchedulerSpaceShared, NestedLimitsTrackTheSameBeneficiaryIndependently)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto outer_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    outer_limit->attachChild(policy);

    auto inner_limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 7000);
    inner_limit->basename = "inner_limit";
    auto inner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    inner_queue->basename = "inner_queue";
    AllocationQueue * inner_queue_ptr = inner_queue.get();
    inner_limit->attachChild(inner_queue);
    policy->attachChild(inner_limit);

    auto outer_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    outer_queue->basename = "outer_queue";
    AllocationQueue * outer_queue_ptr = outer_queue.get();
    policy->attachChild(outer_queue);

    r.root_node = outer_limit;
    outer_queue.reset();
    inner_queue.reset();
    inner_limit.reset();
    policy.reset();
    outer_limit.reset();
    r.registerResource();

    ManualAllocation inner_heavy(inner_queue_ptr, "inner_heavy", 6000);
    ManualAllocation outer_heavy(outer_queue_ptr, "outer_heavy", 3000);

    std::promise<void> inner_entered;
    std::promise<void> inner_release;
    t.scheduler.event_queue.enqueue([&] { inner_entered.set_value(); inner_release.get_future().get(); });
    inner_entered.get_future().get();

    inner_heavy.increaseAsync(2000);
    auto inner_beneficiary = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "inner_beneficiary", 500, /* wait_for_admission = */ false);
    inner_release.set_value();
    inner_beneficiary->waitSynced();
    EXPECT_EQ(inner_heavy.killCount(), 0u);

    std::promise<void> outer_entered;
    std::promise<void> outer_release;
    t.scheduler.event_queue.enqueue([&] { outer_entered.set_value(); outer_release.get_future().get(); });
    outer_entered.get_future().get();

    outer_heavy.increaseAsync(8000); // Impossible even after the inner branch releases.
    auto shared_beneficiary = std::make_unique<ManualAllocation>(
        inner_queue_ptr, "shared_beneficiary", 200, /* wait_for_admission = */ false);
    outer_release.set_value();
    shared_beneficiary->waitSynced();

    EXPECT_EQ(inner_heavy.killCount(), 0u);
    EXPECT_EQ(outer_heavy.killCount(), 0u);

    shared_beneficiary.reset();
    inner_beneficiary.reset();

    /// The inner last resort releases its branch. The outer impossible growth must then reach its
    /// own last resort; a leaked nested beneficiary membership would leave it suspended forever.
    EXPECT_TRUE(inner_heavy.waitKillsFor(1, std::chrono::seconds(5)))
        << "inner limit lost its last-resort suction decision; inner kills="
        << inner_heavy.killCount() << ", outer kills=" << outer_heavy.killCount();
    EXPECT_TRUE(outer_heavy.waitKillsFor(1, std::chrono::seconds(5)))
        << "outer limit lost its last-resort suction decision; inner kills="
        << inner_heavy.killCount() << ", outer kills=" << outer_heavy.killCount();
}


TEST(SchedulerSpaceShared, FairHierarchySearchesPastNonFittingSibling)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    auto heavy_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    heavy_queue->basename = "heavy_queue";
    AllocationQueue * heavy_queue_ptr = heavy_queue.get();
    policy->attachChild(heavy_queue);

    auto blocked_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    blocked_queue->basename = "blocked_queue";
    AllocationQueue * blocked_queue_ptr = blocked_queue.get();
    policy->attachChild(blocked_queue);

    auto fitting_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    fitting_queue->basename = "fitting_queue";
    AllocationQueue * fitting_queue_ptr = fitting_queue.get();
    policy->attachChild(fitting_queue);

    r.root_node = limit;
    fitting_queue.reset();
    blocked_queue.reset();
    heavy_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(heavy_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto blocked = std::make_unique<ManualAllocation>(
        blocked_queue_ptr, "blocked", 3000, /* wait_for_admission = */ false);
    auto fitting = std::make_unique<ManualAllocation>(
        fitting_queue_ptr, "fitting", 1000, /* wait_for_admission = */ false);
    release.set_value();

    fitting->waitSynced();
    EXPECT_EQ(fitting->size(), 1000);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);

    fitting.reset();
    heavy.waitKills(1);
}


/// Suction is a distinct external decision. Arrivals submitted before that decision is released must
/// all receive their policy-scoped fit check before the protected growth can reach eviction.
TEST(SchedulerSpaceShared, ConcurrentFittingArrivalsPrecedeExternalSuction)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.protectAfterPressureRounds(2);
    heavy.increaseAsync(5000);
    heavy.waitPressureCount(1);

    constexpr size_t query_count = 8;
    std::barrier<> start(query_count + 1);
    std::barrier<> submitted(query_count + 1);
    std::vector<std::unique_ptr<ManualAllocation>> fitting(query_count);
    std::vector<std::thread> threads;
    threads.reserve(query_count);

    for (size_t index = 0; index < query_count; ++index)
    {
        threads.emplace_back([&, index]
        {
            start.arrive_and_wait();
            fitting[index] = std::make_unique<ManualAllocation>(
                queue, fmt::format("fitting_{}", index), 250, false);
            submitted.arrive_and_wait();
            fitting[index]->waitSynced();
        });
    }

    start.arrive_and_wait();
    submitted.arrive_and_wait();

    /// This models the query controller's explicit no-reclaim completion. All fitting arrivals are
    /// already queue events; the second pressure observation injects suction only after their search.
    heavy.recoveryCheckpoint();

    for (auto & thread : threads)
        thread.join();

    for (const auto & allocation : fitting)
        EXPECT_EQ(allocation->size(), 250);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Parking is only a step before eviction. If a blocked regular increase has no request behind it, it is
/// immediately restored and the next evaluation follows the existing hard-limit kill policy.
TEST(SchedulerSpaceShared, BlockedGrowthWithoutBeneficiaryStillKills)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.increaseAsync(5000);
    heavy.waitKills(1);

    EXPECT_EQ(heavy.killCount(), 1u);
    EXPECT_EQ(heavy.size(), 8000);
}



/// A repeated no-progress event raises query-scoped priority outside the allocation hierarchy.
/// The protected request then uses the existing in-scope victim policy; it does not bypass the limit.
TEST(SchedulerSpaceShared, RepeatedSuspensionInjectsExternalPriority)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 4000);
    heavy.protectAfterPressureRounds(2);
    auto victim = std::make_unique<ManualAllocation>(queue, "victim", 5000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(4000);
    auto anchor = std::make_unique<ManualAllocation>(queue, "anchor", 500, /* wait_for_admission = */ false);
    release.set_value();

    anchor->waitSynced();
    heavy.waitPressureCount(1);
    ASSERT_EQ(heavy.killCount(), 0u);
    ASSERT_EQ(victim->killCount(), 0u);

    /// A real resource-state change retries the same parked growth. The external controller now
    /// injects protection, so existing in-scope eviction chooses the larger competing allocation.
    victim->decreaseAsync(100);
    victim->waitSynced();
    ASSERT_TRUE(victim->waitKillsFor(1, std::chrono::seconds(5)))
        << "External suction priority did not reach the larger in-scope victim; pressure_events="
        << heavy.pressureCount() << ", heavy_kills=" << heavy.killCount()
        << ", victim_kills=" << victim->killCount();

    EXPECT_EQ(heavy.pressureCount(), 2u);
    EXPECT_EQ(heavy.killCount(), 0u);
    EXPECT_EQ(victim->killCount(), 1u);

    victim.reset();
    heavy.waitSynced();

    EXPECT_EQ(heavy.size(), 8000);
    EXPECT_EQ(anchor->size(), 500);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// With no beneficiary, an externally managed recovery lane gets one query-progress checkpoint
/// before eviction. This is the path that lets a single-threaded query reach forced spilling.
TEST(SchedulerSpaceShared, RecoveryLaneRunsBeforeSuctionBackstop)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.protectAfterPressureRounds(2);
    heavy.increaseAsync(5000);

    heavy.waitPressureCount(1);
    EXPECT_EQ(heavy.killCount(), 0u);

    heavy.recoveryCheckpoint();
    heavy.waitKills(1);

    EXPECT_EQ(heavy.pressureCount(), 2u);
    EXPECT_EQ(heavy.killCount(), 1u);
}



/// A no-candidate or completed-spill notification may arrive immediately from the query thread
/// while the scheduler is still publishing the parked owner. That explicit completion must not be
/// dropped: it must reopen the same episode and reach suction when growth remains impossible.
TEST(SchedulerSpaceShared, EarlyRecoveryCompletionCannotBeLost)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.protectAfterPressureRounds(2);
    heavy.runOnNextPressure([&] { heavy.recoveryCheckpoint(); });
    heavy.increaseAsync(5000);

    ASSERT_TRUE(heavy.waitPressureCountFor(1, std::chrono::seconds(5)))
        << "The initial pressure episode was never published";
    ASSERT_TRUE(heavy.waitKillsFor(1, std::chrono::seconds(5)))
        << "An immediate recovery completion was lost before the queue published its parked owner; "
        << "pressure_events=" << heavy.pressureCount() << ", kills=" << heavy.killCount();

    EXPECT_EQ(heavy.pressureCount(), 2u);
    EXPECT_EQ(heavy.killCount(), 1u);
}


class ManualSpillProcessor final : public IProcessor
{
public:
    ManualSpillProcessor(Int64 spillable_bytes_, bool spill_succeeds_)
        : spillable_bytes(spillable_bytes_)
        , spill_succeeds(spill_succeeds_)
    {
        spillable = true;
    }

    String getName() const override { return "ManualSpillProcessor"; }

    ProcessorMemoryStats getMemoryStats() override
    {
        return {.spillable_memory_bytes = spillable_bytes, .need_reserved_memory_bytes = 0};
    }

    bool spillOnSize(size_t bytes) override
    {
        ++spill_calls;
        last_spill_size = bytes;
        return spill_succeeds;
    }

    size_t spillCallCount() const { return spill_calls; }
    size_t lastSpillSize() const { return last_spill_size; }

private:
    Int64 spillable_bytes;
    bool spill_succeeds;
    size_t spill_calls = 0;
    size_t last_spill_size = 0;
};

class ManualNonSpillProcessor final : public IProcessor
{
public:
    String getName() const override { return "ManualNonSpillProcessor"; }
};

class BlockingSpillProcessor final : public IProcessor
{
public:
    BlockingSpillProcessor(std::promise<void> & entered_, std::shared_future<void> release_)
        : entered(entered_)
        , release(std::move(release_))
    {
        spillable = true;
    }

    String getName() const override { return "BlockingSpillProcessor"; }

    ProcessorMemoryStats getMemoryStats() override
    {
        return {.spillable_memory_bytes = 4096, .need_reserved_memory_bytes = 0};
    }

    bool spillOnSize(size_t) override
    {
        entered.set_value();
        release.wait();
        return true;
    }

private:
    std::promise<void> & entered;
    std::shared_future<void> release;
};


/// Recovery completion belongs to one embedded increase generation and one exact controller
/// epoch. A stale or future notification cannot release the current parked request, and the same
/// completion cannot be consumed twice.
TEST(SchedulerSpaceShared, ReservationAcceptsOnlyExactRecoveryEpochOnce)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;
    ManualSpillProcessor processor(4096, /*spill_succeeds_=*/ true);
    auto scheduler = std::make_shared<MemorySpillScheduler>(/*enable_=*/ false);
    scheduler->registerProcessor(&processor);
    scheduler->setProcessorRunnable(&processor, true);
    MemoryReservation reservation(link, "reservation", 0);
    reservation.bindMemorySpillScheduler(scheduler);

    ResourceAllocation & allocation = reservation;
    ASSERT_EQ(allocation.onGrowthPressure(), ResourceAllocation::GrowthPressureAction::Yield);
    EXPECT_FALSE(allocation.acceptRecoveryProgress(2));
    EXPECT_TRUE(allocation.acceptRecoveryProgress(1));
    EXPECT_FALSE(allocation.acceptRecoveryProgress(1));
    allocation.onGrowthPressureResolved();
}


/// Worker wakeups and memory-sync calls cannot advance pressure. Suction becomes eligible only
/// after the selected processor explicitly finishes the forced spill attempt.
TEST(SchedulerSpaceShared, ExplicitSpillCompletionControlsSuctionPriority)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor processor(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&processor);
    scheduler.setProcessorRunnable(&processor, true);

    const auto first_request = scheduler.requestForcedSpill();
    ASSERT_GT(first_request.epoch, 0u);
    EXPECT_FALSE(first_request.inject_priority);
    EXPECT_EQ(scheduler.getForcedSpillResult(first_request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    /// Re-observing the same blocked request before reclaim completion must not inject priority.
    const auto repeated_request = scheduler.requestForcedSpill();
    EXPECT_EQ(repeated_request.epoch, first_request.epoch);
    EXPECT_FALSE(repeated_request.inject_priority);

    EXPECT_TRUE(scheduler.checkAndSpill(&processor))
        << "The executor must consume this task as recovery-only work";
    EXPECT_EQ(processor.spillCallCount(), 1u);
    EXPECT_EQ(processor.lastSpillSize(), 4096u);
    EXPECT_EQ(scheduler.getForcedSpillResult(first_request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);

    const auto suction_request = scheduler.requestForcedSpill();
    EXPECT_EQ(suction_request.epoch, first_request.epoch);
    EXPECT_TRUE(suction_request.inject_priority);
}


/// Reservations are fine-grained scheduler nodes, but reservations in one dependency graph share
/// its spill controller. Resolving one node cannot close the graph's recovery lane while another
/// blocked node still depends on the same epoch.
TEST(SchedulerSpaceShared, SharedGraphPressureEndsAfterLastRequester)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor processor(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&processor);
    scheduler.setProcessorRunnable(&processor, true);

    const auto first = scheduler.requestForcedSpill(/*register_requester=*/ true);
    const auto second = scheduler.requestForcedSpill(/*register_requester=*/ true);
    ASSERT_EQ(first.epoch, second.epoch);
    ASSERT_FALSE(first.inject_priority);
    ASSERT_FALSE(second.inject_priority);

    scheduler.finishMemoryPressure(/*unregister_requester=*/ true);
    const auto still_pending = scheduler.requestForcedSpill();
    EXPECT_EQ(still_pending.epoch, first.epoch);
    EXPECT_FALSE(still_pending.inject_priority);

    EXPECT_TRUE(scheduler.checkAndSpill(&processor));
    EXPECT_TRUE(scheduler.requestForcedSpill().inject_priority);
    scheduler.finishMemoryPressure(/*unregister_requester=*/ true);

    const auto next_graph_episode = scheduler.requestForcedSpill(/*register_requester=*/ true);
    EXPECT_GT(next_graph_episode.epoch, first.epoch);
    scheduler.finishMemoryPressure(/*unregister_requester=*/ true);
}


/// A forced recovery task cannot continue into ordinary processor work, which may allocate the
/// memory that was just reclaimed. Once the pressure episode ends, the same task is ordinary work
/// again; the executor uses this return value as the boundary.
TEST(SchedulerSpaceShared, ForcedSpillTaskDoesNotRunOrdinaryWork)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor processor(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&processor);
    scheduler.setProcessorRunnable(&processor, true);

    const auto request = scheduler.requestForcedSpill();
    EXPECT_TRUE(scheduler.checkAndSpill(&processor));
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);

    /// Completion only hands the result to the allocation scheduler. Ordinary processor work must
    /// remain parked until that scheduler resolves the graph-wide pressure episode.
    EXPECT_TRUE(scheduler.checkAndSpill(&processor));

    scheduler.finishMemoryPressure();
    EXPECT_FALSE(scheduler.checkAndSpill(&processor));
}


/// A task which cannot spill may still allocate in ordinary execution. It therefore remains in
/// the recovery lane until the graph-wide pressure episode ends, even after another task has
/// completed the explicit spill attempt.
TEST(SchedulerSpaceShared, NonSpillableTaskStaysParkedForWholePressureEpisode)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor spillable(4096, /*spill_succeeds_=*/ true);
    ManualNonSpillProcessor ordinary;
    scheduler.registerProcessor(&spillable);
    scheduler.setProcessorRunnable(&spillable, true);

    const auto request = scheduler.requestForcedSpill();
    EXPECT_TRUE(scheduler.checkAndSpill(&ordinary));
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    EXPECT_TRUE(scheduler.checkAndSpill(&spillable));
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);
    EXPECT_TRUE(scheduler.checkAndSpill(&ordinary));

    scheduler.finishMemoryPressure();
    EXPECT_FALSE(scheduler.checkAndSpill(&ordinary));
}


/// A forced-spill epoch belongs to the query, not to a processor cached during a previous episode.
/// If another spillable processor is the one that becomes runnable, it must be able to claim and
/// complete the new epoch instead of waiting forever for the stale previous winner.
TEST(SchedulerSpaceShared, RunnableProcessorClaimsForcedSpillEpoch)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor previously_selected(8192, /*spill_succeeds_=*/ true);
    ManualSpillProcessor runnable(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&previously_selected);
    scheduler.registerProcessor(&runnable);
    scheduler.setProcessorRunnable(&previously_selected, true);

    const auto first = scheduler.requestForcedSpill();
    scheduler.checkAndSpill(&previously_selected);
    ASSERT_NE(
        scheduler.getForcedSpillResult(first.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);
    scheduler.finishMemoryPressure();

    scheduler.setProcessorRunnable(&previously_selected, false);
    scheduler.setProcessorRunnable(&runnable, true);
    const auto second = scheduler.requestForcedSpill();
    ASSERT_FALSE(second.inject_priority);
    scheduler.checkAndSpill(&runnable);

    EXPECT_EQ(runnable.spillCallCount(), 1u)
        << "The runnable processor could not claim the query-level forced-spill epoch";
    EXPECT_NE(
        scheduler.getForcedSpillResult(second.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending)
        << "The epoch remained pinned to a processor that did not run";
}


/// An executor can disappear after one of its processors claims an epoch but before spill returns.
/// Removing that claimant must offer the same epoch to another runnable processor; the stale
/// completion must not overwrite the replacement's result or pin pressure to a dead address.
TEST(SchedulerSpaceShared, RemovedClaimantReoffersForcedSpillEpoch)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    std::promise<void> claimant_entered;
    auto claimant_entered_future = claimant_entered.get_future();
    std::promise<void> release_claimant;
    BlockingSpillProcessor claimant(claimant_entered, release_claimant.get_future().share());
    ManualSpillProcessor replacement(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&claimant);
    scheduler.registerProcessor(&replacement);
    scheduler.setProcessorRunnable(&claimant, true);
    scheduler.setProcessorRunnable(&replacement, true);

    const auto request = scheduler.requestForcedSpill();
    std::atomic<bool> claimant_consumed{false};
    std::thread claimant_thread([&] { claimant_consumed = scheduler.checkAndSpill(&claimant); });
    if (claimant_entered_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        release_claimant.set_value();
        claimant_thread.join();
        FAIL() << "The first runnable processor never claimed the forced-spill epoch";
    }

    scheduler.beginForcedSpillScan();
    scheduler.remove(&claimant);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    release_claimant.set_value();
    claimant_thread.join();
    EXPECT_TRUE(claimant_consumed.load());
    EXPECT_TRUE(scheduler.checkAndSpill(&replacement));
    EXPECT_EQ(replacement.spillCallCount(), 1u);
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);
}



/// A query with no registered spillable processor has a concrete no-candidate result. It reaches
/// the suction/eviction backstop without a timer and without pretending a worker sync reclaimed data.
TEST(SchedulerSpaceShared, NoSpillCandidateReachesSuctionBackstop)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);

    const auto spill_request = scheduler.requestForcedSpill();
    EXPECT_FALSE(spill_request.inject_priority);
    EXPECT_EQ(scheduler.getForcedSpillResult(spill_request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);

    const auto suction_request = scheduler.requestForcedSpill();
    EXPECT_TRUE(suction_request.inject_priority);
}


/// A forced spill must shrink the parked reservation request before retry. The original +5000
/// request no longer describes demand after reclaim leaves only +1000 outstanding.
TEST(SchedulerSpaceShared, ForcedSpillReconcilesParkedIncrease)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.protectAfterPressureRounds(2);
    heavy.increaseAsync(5000);

    heavy.waitPressureCount(1);
    heavy.reconcilePendingIncreaseTo(1000);
    heavy.recoveryCheckpoint();
    heavy.waitSynced();

    EXPECT_EQ(heavy.size(), 9000);
    EXPECT_EQ(heavy.pressureCount(), 1u);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Exercise the production reservation callback path rather than ManualAllocation's configurable
/// pressure response. Heavy and Small are independent dependency graphs sharing one workload queue,
/// so each blocked graph must receive its own explicit forced-spill attempt before queue-level suction
/// may evict Heavy. This covers the non-owner blocked-growth handoff in the real yield-first path.
TEST(SchedulerSpaceShared, RealReservationsGiveEveryBlockedGraphARecoveryPassBeforeEviction)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ResourceLink link;
    link.allocation_queue = queue;

    ManualSpillProcessor heavy_processor(4096, /*spill_succeeds_=*/ false);
    ManualSpillProcessor small_processor(4096, /*spill_succeeds_=*/ false);
    /// Declare controllers after their raw-pointer processor registrations so the controllers are
    /// destroyed first on every return path.
    auto heavy_spill_scheduler = std::make_shared<MemorySpillScheduler>(/*enable_=*/ false);
    auto small_spill_scheduler = std::make_shared<MemorySpillScheduler>(/*enable_=*/ false);
    heavy_spill_scheduler->registerProcessor(&heavy_processor);
    heavy_spill_scheduler->setProcessorRunnable(&heavy_processor, true);
    small_spill_scheduler->registerProcessor(&small_processor);
    small_spill_scheduler->setProcessorRunnable(&small_processor, true);

    auto heavy = std::make_unique<MemoryReservation>(link, "heavy", 8000);
    heavy->bindMemorySpillScheduler(heavy_spill_scheduler);
    MemoryTracker heavy_tracker;
    heavy_tracker.adjustWithUntrackedMemory(13000);

    auto detach_resource = [&]
    {
        /// Detaching first is required by AllocationQueue::purgeQueue(). It also makes the
        /// reservation failure durable before the queue object is destroyed on the scheduler
        /// thread, so every blocked query thread below is guaranteed to wake for cleanup.
        r.unregisterResource();
    };

    auto wait_until_suspended = [&](const ResourceAllocation & allocation)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto promise = std::make_shared<std::promise<bool>>();
            auto future = promise->get_future();
            t.scheduler.event_queue.enqueue([&allocation, promise]
            {
                promise->set_value(allocation.isIncreaseSuspended());
            });
            if (future.wait_for(std::chrono::seconds(1)) == std::future_status::ready && future.get())
                return true;
            std::this_thread::yield();
        }
        return false;
    };

    auto wait_until_recovery_active = [&](ResourceAllocation & allocation, bool expected)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto promise = std::make_shared<std::promise<bool>>();
            auto future = promise->get_future();
            t.scheduler.event_queue.enqueue([&allocation, promise]
            {
                promise->set_value(allocation.isGrowthRecoveryActive());
            });
            if (future.wait_for(std::chrono::seconds(1)) == std::future_status::ready
                && future.get() == expected)
                return true;
            std::this_thread::yield();
        }
        return false;
    };

    auto complete_forced_spill = [](MemorySpillScheduler & spill_scheduler, ManualSpillProcessor & processor)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (spill_scheduler.checkAndSpill(&processor) && processor.spillCallCount() > 0)
                return true;
            std::this_thread::yield();
        }
        return false;
    };

    auto read_kill_requested = [&](const ResourceAllocation & allocation)
    {
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();
        t.scheduler.event_queue.enqueue([&allocation, promise]
        {
            promise->set_value(allocation.isKillRequested());
        });
        if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
            return std::optional<bool>{};
        return std::optional<bool>{future.get()};
    };

    /// The first sync returns through MemoryReservation's recovery-only lane. Keep it bounded too:
    /// a broken scheduler-to-query hand-off is test evidence, not permission to hang the test suite.
    std::promise<void> heavy_recovery_done;
    auto heavy_recovery_done_future = heavy_recovery_done.get_future();
    std::atomic<int> heavy_recovery_exception_code{0};
    std::thread heavy_recovery_thread([&]
    {
        try
        {
            heavy->syncWithMemoryTracker(&heavy_tracker);
        }
        catch (const DB::Exception & e)
        {
            heavy_recovery_exception_code = e.code();
        }
        catch (...) // Ok: report unexpected exceptions to the main test thread
        {
            heavy_recovery_exception_code = -1;
        }
        heavy_recovery_done.set_value();
    });

    if (!wait_until_suspended(*heavy))
    {
        ADD_FAILURE() << "The real reservation never entered its yield-first recovery episode";
        detach_resource();
        heavy_recovery_done_future.wait();
        heavy_recovery_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        return;
    }
    if (heavy_recovery_done_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        ADD_FAILURE() << "Heavy did not return through its recovery-only lane";
        detach_resource();
        heavy_recovery_done_future.wait();
        heavy_recovery_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        return;
    }
    heavy_recovery_thread.join();
    if (heavy_recovery_exception_code.load() != 0)
    {
        ADD_FAILURE() << "Heavy failed before it could attempt recovery; code="
                      << heavy_recovery_exception_code.load();
        detach_resource();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        return;
    }
    if (!complete_forced_spill(*heavy_spill_scheduler, heavy_processor))
    {
        ADD_FAILURE() << "Heavy did not publish a forced-spill epoch";
        detach_resource();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        return;
    }
    EXPECT_EQ(heavy_processor.spillCallCount(), 1u);

    auto small = std::make_unique<MemoryReservation>(link, "small", 1000);
    small->bindMemorySpillScheduler(small_spill_scheduler);
    MemoryTracker small_tracker;
    small_tracker.adjustWithUntrackedMemory(3000);

    std::promise<void> small_done;
    auto small_done_future = small_done.get_future();
    std::atomic<int> small_exception_code{0};
    std::thread small_thread([&]
    {
        try
        {
            small->syncWithMemoryTracker(&small_tracker);
        }
        catch (const DB::Exception & e)
        {
            small_exception_code = e.code();
        }
        catch (...) // Ok: report unexpected exceptions to the main test thread
        {
            small_exception_code = -1;
        }
        small_done.set_value();
    });

    if (!wait_until_suspended(*small))
    {
        ADD_FAILURE() << "The admitted beneficiary's blocked growth never reached the scheduler";
        detach_resource();
        small_done_future.wait();
        small_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    if (small_done_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        ADD_FAILURE() << "Small did not return through its recovery-only lane";
        detach_resource();
        small_done_future.wait();
        small_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }
    small_thread.join();
    if (small_exception_code.load() != 0)
    {
        ADD_FAILURE() << "Small failed before it could report recovery completion; code="
                      << small_exception_code.load();
        detach_resource();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    if (!wait_until_recovery_active(*small, true))
    {
        ADD_FAILURE() << "Small returned without entering its recovery-only lane";
        detach_resource();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    /// Publish Heavy's completed attempt while Small is still inside its own pending recovery pass.
    /// This is the critical ordering: Heavy's externally authorized suction must not evict either
    /// graph until the independent Small graph has actually been offered its recovery task.
    std::promise<void> heavy_done;
    auto heavy_done_future = heavy_done.get_future();
    std::atomic<int> heavy_exception_code{0};
    std::atomic<bool> stop_heavy{false};
    std::thread heavy_thread([&]
    {
        try
        {
            while (!stop_heavy.load(std::memory_order_relaxed))
            {
                heavy->syncWithMemoryTracker(&heavy_tracker);
                std::this_thread::yield();
            }
        }
        catch (const DB::Exception & e)
        {
            heavy_exception_code = e.code();
        }
        catch (...) // Ok: report unexpected exceptions to the main test thread
        {
            heavy_exception_code = -1;
        }
        heavy_done.set_value();
    });

    if (!wait_until_recovery_active(*heavy, false))
    {
        ADD_FAILURE() << "Heavy's explicit spill completion was not consumed by the scheduler";
        detach_resource();
        heavy_done_future.wait();
        heavy_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    const auto heavy_kill_requested = read_kill_requested(*heavy);
    if (!heavy_kill_requested.has_value())
    {
        ADD_FAILURE() << "Could not inspect scheduler-owned victim state";
        stop_heavy.store(true, std::memory_order_relaxed);
        detach_resource();
        heavy_done_future.wait();
        heavy_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }
    if (*heavy_kill_requested)
    {
        ADD_FAILURE() << "Heavy was selected for eviction before the non-owner graph received its recovery pass";
        stop_heavy.store(true, std::memory_order_relaxed);
        detach_resource();
        heavy_done_future.wait();
        heavy_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    if (!complete_forced_spill(*small_spill_scheduler, small_processor))
    {
        ADD_FAILURE() << "The non-owner dependency graph never received its own recovery pass";
        detach_resource();
        heavy_done_future.wait();
        heavy_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }
    EXPECT_EQ(small_processor.spillCallCount(), 1u);

    /// A second sync publishes Small's explicit NoProgress result. It may then block waiting for the
    /// final queue decision, so keep it on its query thread while observing scheduler-owned state.
    std::promise<void> small_retry_done;
    auto small_retry_done_future = small_retry_done.get_future();
    std::atomic<int> small_retry_exception_code{0};
    std::thread small_retry_thread([&]
    {
        try
        {
            small->syncWithMemoryTracker(&small_tracker);
        }
        catch (const DB::Exception & e)
        {
            small_retry_exception_code = e.code();
        }
        catch (...) // Ok: report unexpected exceptions to the main test thread
        {
            small_retry_exception_code = -1;
        }
        small_retry_done.set_value();
    });

    if (!wait_until_recovery_active(*small, false))
    {
        ADD_FAILURE() << "Small's explicit spill completion was not consumed by the scheduler";
        detach_resource();
        heavy_done_future.wait();
        small_retry_done_future.wait();
        heavy_thread.join();
        small_retry_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    if (heavy_done_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        ADD_FAILURE() << "The parked owner never reached the deterministic eviction backstop";
        stop_heavy.store(true, std::memory_order_relaxed);
        detach_resource();
        heavy_done_future.wait();
        small_retry_done_future.wait();
        heavy_thread.join();
        small_retry_thread.join();
        heavy_tracker.adjustWithUntrackedMemory(-13000);
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }

    heavy_thread.join();
    EXPECT_EQ(heavy_exception_code.load(), ErrorCodes::MEMORY_RESERVATION_KILLED)
        << "Blocked beneficiary growth selected the wrong victim";

    /// Kill notification alone does not release memory; query teardown does. Destroy Heavy to publish
    /// the ordinary decrease, after which Small's already-queued +2000 request must be approved.
    heavy.reset();
    heavy_tracker.adjustWithUntrackedMemory(-13000);

    if (small_retry_done_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
    {
        ADD_FAILURE() << "Small did not resume after the killed owner released its reservation";
        detach_resource();
        small_retry_done_future.wait();
        small_retry_thread.join();
        small_tracker.adjustWithUntrackedMemory(-3000);
        return;
    }
    small_retry_thread.join();
    EXPECT_EQ(small_retry_exception_code.load(), 0)
        << "The beneficiary was killed or failed instead of receiving the released capacity";

    auto snapshot = getSchedulerSnapshot(t, *queue);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->root_allocated, 3000);
    EXPECT_EQ(snapshot->queue_allocated, 3000);

    small_tracker.adjustWithUntrackedMemory(-3000);
}


/// Property-style stress test for the flow invariant. A permanently impossible growth request is
/// kept alive while a deterministic random stream mixes:
/// - pending allocations, including batches queued behind the blocked growth;
/// - fitting growth of already admitted allocations;
/// - partial releases;
/// - short- and long-lived allocations.
///
/// Every generated request is bounded by the free capacity observed by the model. Therefore every
/// one must be approved, no fitting allocation may be killed, and accounted usage must stay within
/// the hard limit. Fixed seeds make failures exactly reproducible.
TEST(SchedulerSpaceShared, RandomizedFittingAllocationsAlwaysProgress)
{
    constexpr ResourceCost limit = 10000;
    constexpr size_t rounds = 32;
    std::vector<UInt64> seeds{
        0x13579BDFULL,
        0x5EED1234ULL,
        0xC0FFEE42ULL,
        0xDEADBEEFULL,
    };
    if (const char * seed_from_environment = std::getenv("CLICKHOUSE_SCHEDULER_RANDOM_SEED")) // NOLINT(concurrency-mt-unsafe)
    {
        char * parse_end = nullptr;
        UInt64 base_seed = static_cast<UInt64>(std::strtoull(seed_from_environment, &parse_end, 10));
        ASSERT_NE(parse_end, seed_from_environment);
        ASSERT_EQ(*parse_end, 0);

        size_t iterations = 1;
        if (const char * iterations_from_environment = std::getenv("CLICKHOUSE_SCHEDULER_RANDOM_ITERATIONS")) // NOLINT(concurrency-mt-unsafe)
        {
            parse_end = nullptr;
            iterations = static_cast<size_t>(std::strtoull(iterations_from_environment, &parse_end, 10));
            ASSERT_NE(parse_end, iterations_from_environment);
            ASSERT_EQ(*parse_end, 0);
            ASSERT_GT(iterations, 0u);
            ASSERT_LE(iterations, 1000000u);
        }

        seeds.clear();
        seeds.reserve(iterations);
        for (size_t iteration = 0; iteration < iterations; ++iteration)
            seeds.push_back(base_seed + iteration);
    }

    const bool report_metrics = std::getenv("CLICKHOUSE_SCHEDULER_REPORT_METRICS") != nullptr; // NOLINT(concurrency-mt-unsafe)
    const auto benchmark_started = std::chrono::steady_clock::now();
    size_t total_fitting_requests_approved = 0;
    size_t total_progress_events = 0;
    size_t total_release_retry_checkpoints = 0;
    size_t total_queries_completed = 0;
    size_t peak_live_queries = 0;
    ResourceCost peak_allocated = 0;
    ResourceCost total_fitting_bytes_approved = 0;
    ResourceCost total_bytes_released = 0;
    size_t total_last_resort_kills = 0;

    for (UInt64 seed : seeds)
    {
        SCOPED_TRACE(fmt::format("seed={}", seed));

        SpaceSharedTest t;
        SpaceSharedResourceHolder r(t);
        r.addLimit("/", limit);
        AllocationQueue * queue = r.addQueue("/queue");
        r.registerResource();

        std::mt19937_64 rng(seed);
        auto randomBetween = [&](ResourceCost minimum, ResourceCost maximum)
        {
            std::uniform_int_distribution<ResourceCost> distribution(minimum, maximum);
            return distribution(rng);
        };

        ManualAllocation heavy(queue, "heavy", 5000);
        auto releaser = std::make_unique<ManualAllocation>(queue, "releaser", 3000);

        std::promise<void> entered;
        std::promise<void> release;
        t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
        entered.get_future().get();

        heavy.increaseAsync(6000); // 5000 + 6000 > limit even when heavy is alone.
        auto anchor = std::make_unique<ManualAllocation>(queue, "anchor", 200, /* wait_for_admission = */ false);
        release.set_value();
        anchor->waitSynced();

        struct LiveQuery
        {
            std::unique_ptr<ManualAllocation> allocation;
            size_t rounds_left;
        };

        std::vector<LiveQuery> live_queries;
        size_t fitting_requests_approved = 1; // The anchor.
        size_t progress_events = 0;
        size_t release_retry_checkpoints = 0;
        size_t queries_completed = 0;
        ResourceCost fitting_bytes_approved = anchor->size();
        ResourceCost bytes_released = 0;

        auto totalAllocated = [&]()
        {
            ResourceCost total = heavy.size() + releaser->size() + anchor->size();
            for (auto & query : live_queries)
                total += query.allocation->size();
            return total;
        };
        peak_allocated = std::max(peak_allocated, totalAllocated());

        for (size_t round = 0; round < rounds; ++round)
        {
            /// Complete queries whose randomized lifetime expired. The anchor remains alive, so
            /// these releases retry/re-park heavy growth without exhausting suspension.
            for (auto it = live_queries.begin(); it != live_queries.end();)
            {
                if (--it->rounds_left == 0)
                {
                    ResourceCost completed_size = it->allocation->size();
                    it = live_queries.erase(it);
                    ++progress_events;
                    ++release_retry_checkpoints;
                    ++queries_completed;
                    bytes_released += completed_size;
                }
                else
                    ++it;
            }

            /// Guarantee repeated progress checkpoints for the long-lived blocked growth.
            if (round % 4 == 0)
            {
                ResourceCost release_size = randomBetween(50, 200);
                release_size = std::min(release_size, releaser->size());
                releaser->decreaseAsync(release_size);
                releaser->waitSynced();
                ++progress_events;
                ++release_retry_checkpoints;
                bytes_released += release_size;
                ASSERT_EQ(heavy.killCount(), 0u) << "round=" << round;
            }

            ResourceCost free = limit - totalAllocated();
            while (free < 4 && !live_queries.empty())
            {
                ResourceCost completed_size = live_queries.front().allocation->size();
                live_queries.erase(live_queries.begin());
                ++progress_events;
                ++release_retry_checkpoints;
                ++queries_completed;
                bytes_released += completed_size;
                free = limit - totalAllocated();
            }
            ASSERT_GT(free, 0);

            /// Queue a random fitting batch while the scheduler is parked so every request is
            /// simultaneously visible behind the suspended heavy growth.
            size_t max_batch = static_cast<size_t>(std::min<ResourceCost>(4, free));
            size_t batch_size = static_cast<size_t>(randomBetween(1, static_cast<ResourceCost>(max_batch)));
            ResourceCost batch_total = randomBetween(
                static_cast<ResourceCost>(batch_size),
                std::min<ResourceCost>(free, 600));

            std::vector<ResourceCost> expected_sizes;
            expected_sizes.reserve(batch_size);
            ResourceCost remaining = batch_total;
            for (size_t index = 0; index < batch_size; ++index)
            {
                ResourceCost remaining_queries = static_cast<ResourceCost>(batch_size - index - 1);
                ResourceCost size = index + 1 == batch_size
                    ? remaining
                    : randomBetween(1, remaining - remaining_queries);
                expected_sizes.push_back(size);
                remaining -= size;
            }

            std::promise<void> batch_entered;
            std::promise<void> batch_release;
            t.scheduler.event_queue.enqueue([&] { batch_entered.set_value(); batch_release.get_future().get(); });
            batch_entered.get_future().get();

            std::vector<std::unique_ptr<ManualAllocation>> batch;
            batch.reserve(batch_size);
            for (size_t index = 0; index < batch_size; ++index)
            {
                batch.emplace_back(std::make_unique<ManualAllocation>(
                    queue,
                    fmt::format("random_{}_{}_{}", seed, round, index),
                    expected_sizes[index],
                    /* wait_for_admission = */ false));
            }
            batch_release.set_value();

            for (size_t index = 0; index < batch_size; ++index)
            {
                batch[index]->waitSynced();
                ASSERT_EQ(batch[index]->size(), expected_sizes[index]);
                ASSERT_EQ(batch[index]->killCount(), 0u);
                live_queries.push_back({
                    .allocation = std::move(batch[index]),
                    .rounds_left = static_cast<size_t>(randomBetween(1, 8)),
                });
                ++fitting_requests_approved;
                fitting_bytes_approved += expected_sizes[index];
            }
            peak_live_queries = std::max(peak_live_queries, live_queries.size());
            peak_allocated = std::max(peak_allocated, totalAllocated());

            /// Randomly grow one admitted query, but never ask for more than the modelled free
            /// capacity. This covers fitting regular increases as well as fitting admissions.
            free = limit - totalAllocated();
            if (free > 0 && !live_queries.empty() && randomBetween(0, 1) != 0)
            {
                size_t index = static_cast<size_t>(
                    randomBetween(0, static_cast<ResourceCost>(live_queries.size() - 1)));
                ManualAllocation & query = *live_queries[index].allocation;
                ResourceCost increase = randomBetween(1, std::min<ResourceCost>(free, 250));
                ResourceCost old_size = query.size();

                query.increaseAsync(increase);
                query.waitSynced();

                ASSERT_EQ(query.size(), old_size + increase);
                ASSERT_EQ(query.killCount(), 0u);
                ++fitting_requests_approved;
                fitting_bytes_approved += increase;
                peak_allocated = std::max(peak_allocated, totalAllocated());
            }

            /// Randomly release part of a live query. Each release is another resource-state
            /// checkpoint that retries the long-lived growth and may park it again.
            if (!live_queries.empty() && randomBetween(0, 1) != 0)
            {
                size_t index = static_cast<size_t>(
                    randomBetween(0, static_cast<ResourceCost>(live_queries.size() - 1)));
                ManualAllocation & query = *live_queries[index].allocation;
                ResourceCost old_size = query.size();
                if (old_size > 0)
                {
                    ResourceCost decrease = randomBetween(1, old_size);
                    query.decreaseAsync(decrease);
                    query.waitSynced();
                    ASSERT_EQ(query.size(), old_size - decrease);
                    ++progress_events;
                    ++release_retry_checkpoints;
                    bytes_released += decrease;
                }
            }

            ASSERT_LE(totalAllocated(), limit);
            ASSERT_EQ(heavy.killCount(), 0u) << "round=" << round;
            for (auto & query : live_queries)
                ASSERT_EQ(query.allocation->killCount(), 0u);
        }

        EXPECT_GT(fitting_requests_approved, rounds);
        EXPECT_GE(progress_events, rounds / 4);

        for (const auto & query : live_queries)
            bytes_released += query.allocation->size();
        queries_completed += live_queries.size();
        release_retry_checkpoints += live_queries.size();
        live_queries.clear();
        bytes_released += releaser->size();
        ++release_retry_checkpoints;
        releaser.reset();
        anchor.reset();

        /// Once every beneficiary finishes, the heavy request is still impossible and must reach
        /// the existing last-resort kill path rather than remain parked forever.
        heavy.waitKills(1);
        EXPECT_EQ(heavy.killCount(), 1u);

        total_fitting_requests_approved += fitting_requests_approved;
        total_progress_events += progress_events;
        total_release_retry_checkpoints += release_retry_checkpoints;
        total_queries_completed += queries_completed;
        total_fitting_bytes_approved += fitting_bytes_approved;
        total_bytes_released += bytes_released;
        total_last_resort_kills += heavy.killCount();
    }

    if (report_metrics)
    {
        const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - benchmark_started).count();
        std::cout
            << "SCHEDULER_METRICS"
            << " seeds=" << seeds.size()
            << " rounds_per_seed=" << rounds
            << " fitting_requests_approved=" << total_fitting_requests_approved
            << " fitting_bytes_approved=" << total_fitting_bytes_approved
            << " queries_completed=" << total_queries_completed
            << " progress_events=" << total_progress_events
            << " release_retry_checkpoints=" << total_release_retry_checkpoints
            << " peak_live_queries=" << peak_live_queries
            << " peak_allocated=" << peak_allocated
            << " bytes_released=" << total_bytes_released
            << " last_resort_kills=" << total_last_resort_kills
            << " elapsed_us=" << elapsed_us
            << std::endl;
    }
}

/// A late pending request that fits must wake an idle queue even when older pending requests were
/// parked earlier in the same suspension round.
TEST(SchedulerSpaceShared, LateFittingAdmissionWakesSuspendedQueue)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto beneficiary = std::make_unique<ManualAllocation>(queue, "beneficiary", 1000, false);
    auto blocked = std::make_unique<ManualAllocation>(queue, "blocked", 3000, false);
    release.set_value();

    beneficiary->waitSynced();
    ASSERT_EQ(beneficiary->size(), 1000);
    ASSERT_EQ(heavy.killCount(), 0u);

    auto late_fitting = std::make_unique<ManualAllocation>(queue, "late_fitting", 1000, false);
    late_fitting->waitSynced();

    EXPECT_EQ(late_fitting->size(), 1000);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// The equivalent wake-up rule applies to regular growth from an already admitted allocation, not
/// only to newly admitted queries.
TEST(SchedulerSpaceShared, LateFittingRegularGrowthWakesSuspendedQueue)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 7000);
    ManualAllocation late_grower(queue, "late_grower", 500);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto beneficiary = std::make_unique<ManualAllocation>(queue, "beneficiary", 1000, false);
    auto blocked = std::make_unique<ManualAllocation>(queue, "blocked", 3000, false);
    release.set_value();

    beneficiary->waitSynced();
    late_grower.increaseAsync(500);
    late_grower.waitSynced();

    EXPECT_EQ(late_grower.size(), 1000);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Detaching an unrelated empty subtree must not forget a parked request or sever its release-driven
/// retry path.
TEST(SchedulerSpaceShared, UnrelatedDetachKeepsSuspendedGrowthRetryable)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    FairAllocation * policy_ptr = policy.get();
    policy->basename = "policy";
    limit->attachChild(policy);

    auto heavy_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    heavy_queue->basename = "heavy_queue";
    AllocationQueue * heavy_queue_ptr = heavy_queue.get();
    policy->attachChild(heavy_queue);

    auto beneficiary_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    beneficiary_queue->basename = "beneficiary_queue";
    AllocationQueue * beneficiary_queue_ptr = beneficiary_queue.get();
    policy->attachChild(beneficiary_queue);

    auto unrelated_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    unrelated_queue->basename = "unrelated_queue";
    policy->attachChild(unrelated_queue);

    r.root_node = limit;
    beneficiary_queue.reset();
    heavy_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(heavy_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto beneficiary = std::make_unique<ManualAllocation>(
        beneficiary_queue_ptr, "beneficiary", 1000, false);
    release.set_value();
    beneficiary->waitSynced();
    ASSERT_EQ(heavy.killCount(), 0u);

    std::promise<void> detached;
    auto detached_future = detached.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        policy_ptr->removeChild(unrelated_queue.get());
        unrelated_queue.reset();
        detached.set_value();
    });
    detached_future.get();

    beneficiary.reset();
    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
}


/// Without an explicit opt-in pressure policy, suspension must not silently override a precedence
/// boundary. The high-precedence workload reaches its normal last resort; lower-precedence work is
/// not admitted merely because it happens to fit.
TEST(SchedulerSpaceShared, DefaultSuspensionDoesNotCrossPrecedenceBoundary)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<PrecedenceAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    SchedulerNodeInfo high_info;
    high_info.setPrecedence(0);
    auto high_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, high_info);
    high_queue->basename = "high";
    AllocationQueue * high_queue_ptr = high_queue.get();
    policy->attachChild(high_queue);

    SchedulerNodeInfo low_info;
    low_info.setPrecedence(1);
    auto low_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, low_info);
    low_queue->basename = "low";
    AllocationQueue * low_queue_ptr = low_queue.get();
    policy->attachChild(low_queue);

    r.root_node = limit;
    low_queue.reset();
    high_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(high_queue_ptr, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto lower_precedence = std::make_unique<ManualAllocation>(
        low_queue_ptr, "lower_precedence", 1000, false);
    release.set_value();

    heavy.waitKills(1);
    EXPECT_EQ(heavy.killCount(), 1u);
    EXPECT_EQ(lower_precedence->size(), 0);
}


/// Reference half of the request-quantization invariant: one large growth request is stalled and
/// leaves the fixed pressure zone available to fitting work.
TEST(SchedulerSpaceShared, LargeGrowthLeavesProtectedCapacityForFittingWork)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    ManualAllocation releaser(queue, "releaser", 3000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(3000);
    auto small = std::make_unique<ManualAllocation>(queue, "small", 500, false);
    release.set_value();

    small->waitSynced();
    EXPECT_EQ(heavy.size(), 6000);
    EXPECT_EQ(small->size(), 500);
    EXPECT_EQ(heavy.killCount(), 0u);

    releaser.decreaseAsync(3000);
    releaser.waitSynced();
    heavy.waitSynced();
    EXPECT_EQ(heavy.size(), 9000);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// The same intended +3000 growth split at a MemoryTracker sync point must not consume the protected
/// pressure zone before a fitting query gets its chance. Scheduling must not depend on whether growth
/// arrives as 1x3000 or 3x1000.
TEST(SchedulerSpaceShared, SmallStepGrowthLeavesTheSameProtectedCapacity)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 6000);
    ManualAllocation releaser(queue, "releaser", 3000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(1000); // First chunk of an intended +3000 growth.
    auto small = std::make_unique<ManualAllocation>(queue, "small", 500, false);
    release.set_value();

    small->waitSynced();
    EXPECT_EQ(heavy.size(), 6000) << "Growth must stall on entering the fixed pressure zone";
    EXPECT_EQ(small->size(), 500);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Reclamation by the suspended holder is useful progress. It must remain able to decrease, and the
/// resulting headroom must approve its parked growth without killing either allocation.
TEST(SchedulerSpaceShared, SuspendedHolderCanReclaimAndResume)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(3000);
    auto small = std::make_unique<ManualAllocation>(queue, "small", 1000, false);
    release.set_value();
    small->waitSynced();

    heavy.decreaseAsync(2000);
    heavy.waitSynced();

    EXPECT_EQ(heavy.size(), 9000);
    EXPECT_EQ(small->size(), 1000);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Search is complete within the constrained policy subtree: every fitting sibling is admitted before
/// the scheduler considers eviction, even with an earlier non-fitting sibling.
TEST(SchedulerSpaceShared, AllFittingFairSiblingsRunBeforeEviction)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    auto make_queue = [&](const String & name)
    {
        auto queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
        queue->basename = name;
        policy->attachChild(queue);
        return queue;
    };

    auto heavy_queue = make_queue("heavy");
    auto blocked_queue = make_queue("blocked");
    auto fitting_a_queue = make_queue("fitting_a");
    auto fitting_b_queue = make_queue("fitting_b");
    auto fitting_c_queue = make_queue("fitting_c");

    AllocationQueue * heavy_queue_ptr = heavy_queue.get();
    AllocationQueue * blocked_queue_ptr = blocked_queue.get();
    AllocationQueue * fitting_a_queue_ptr = fitting_a_queue.get();
    AllocationQueue * fitting_b_queue_ptr = fitting_b_queue.get();
    AllocationQueue * fitting_c_queue_ptr = fitting_c_queue.get();

    r.root_node = limit;
    fitting_c_queue.reset();
    fitting_b_queue.reset();
    fitting_a_queue.reset();
    blocked_queue.reset();
    heavy_queue.reset();
    policy.reset();
    limit.reset();
    r.registerResource();

    ManualAllocation heavy(heavy_queue_ptr, "heavy", 7000);

    std::promise<void> entered;
    std::promise<void> release;
    t.scheduler.event_queue.enqueue([&] { entered.set_value(); release.get_future().get(); });
    entered.get_future().get();

    heavy.increaseAsync(5000);
    auto blocked = std::make_unique<ManualAllocation>(blocked_queue_ptr, "blocked", 4000, false);
    auto fitting_a = std::make_unique<ManualAllocation>(fitting_a_queue_ptr, "fitting_a", 1000, false);
    auto fitting_b = std::make_unique<ManualAllocation>(fitting_b_queue_ptr, "fitting_b", 500, false);
    auto fitting_c = std::make_unique<ManualAllocation>(fitting_c_queue_ptr, "fitting_c", 500, false);
    release.set_value();

    fitting_a->waitSynced();
    fitting_b->waitSynced();
    fitting_c->waitSynced();

    EXPECT_EQ(fitting_a->size(), 1000);
    EXPECT_EQ(fitting_b->size(), 500);
    EXPECT_EQ(fitting_c->size(), 500);
    EXPECT_EQ(blocked->size(), 0);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Concurrent fitting arrivals must not race with suspension state or remain hidden. Their aggregate
/// request exactly matches the free 2 KB, so every one has a valid admission and no victim is needed.
TEST(SchedulerSpaceShared, ConcurrentFittingArrivalsAllProgress)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 8000);
    heavy.protectAfterPressureRounds(2);
    heavy.increaseAsync(5000);
    heavy.waitPressureCount(1);

    constexpr size_t query_count = 8;
    std::barrier<> start(query_count + 1);
    std::barrier<> submitted(query_count + 1);
    std::vector<std::unique_ptr<ManualAllocation>> fitting(query_count);
    std::vector<std::thread> threads;
    threads.reserve(query_count);

    for (size_t index = 0; index < query_count; ++index)
    {
        threads.emplace_back([&, index]
        {
            start.arrive_and_wait();
            fitting[index] = std::make_unique<ManualAllocation>(
                queue, fmt::format("fitting_{}", index), 250, false);
            submitted.arrive_and_wait();
            fitting[index]->waitSynced();
        });
    }

    start.arrive_and_wait();
    submitted.arrive_and_wait();
    heavy.recoveryCheckpoint();
    for (auto & thread : threads)
        thread.join();

    for (const auto & allocation : fitting)
        EXPECT_EQ(allocation->size(), 250);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Suction work is node-owned state. Detaching the limit after suction is queued must cancel the
/// callback; an event retaining a raw AllocationLimit pointer will be detected as a child-process
/// crash (and as a precise use-after-free under ASan).
TEST(SchedulerSpaceShared, DetachingLimitCancelsQueuedSuction)
{
    ASSERT_EXIT(
        {
            SpaceSharedTest t;
            SpaceSharedResourceHolder r(t);
            r.addLimit("/", 10000);
            AllocationQueue * queue = r.addQueue("/queue");
            r.registerResource();

            /// The child exits without normal object teardown, so this allocation deliberately
            /// remains alive while the scheduler destroys its owning subtree.
            auto * heavy = new ManualAllocation(queue, "heavy", 8000);
            heavy->protectAfterPressureRounds(1);
            heavy->runOnNextPressure([&]
            {
                /// This event is enqueued before the suction event created by the same pressure
                /// turn. It removes and destroys the limit; later suction must therefore be
                /// cancelled rather than dereference the destroyed node.
                t.scheduler.event_queue.enqueue([&]
                {
                    t.scheduler.removeChild(r.root_node.get());
                    r.root_node.reset();
                });
            });

            heavy->increaseAsync(5000);
            if (!heavy->waitPressureCountFor(1, std::chrono::seconds(5)))
                std::_Exit(2);

            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            std::_Exit(0);
        },
        ::testing::ExitedWithCode(0),
        "");
}


/// A retry activation is owned by its AllocationLimit. Destroying the limit in the same scheduler
/// event that queues the retry must cancel that callback; the sentinel is deliberately queued
/// behind it so a stale raw-node activation becomes a deterministic child-process crash.
TEST(SchedulerSpaceShared, DestroyingLimitCancelsQueuedRetryActivation)
{
    ASSERT_EXIT(
        {
            SpaceSharedTest t;
            SpaceSharedResourceHolder r(t);
            AllocationLimit * limit = r.addLimit("/", 10000);
            AllocationQueue * queue = r.addQueue("/queue");
            r.registerResource();

            /// The child exits without teardown; keep this object alive while destruction of the
            /// owning queue reports cancellation into it.
            auto * heavy = new ManualAllocation(queue, "heavy", 8000);
            heavy->protectAfterPressureRounds(1);

            std::promise<void> limit_destroyed;
            auto limit_destroyed_future = limit_destroyed.get_future();
            std::promise<void> queue_drained;
            auto queue_drained_future = queue_drained.get_future();
            heavy->runOnNextPressure([&]
            {
                /// `onGrowthPressure` runs before the limit finishes publishing its owner. This
                /// event therefore observes the completed suspension, queues exactly one retry,
                /// then destroys the node before that queued activation can execute.
                t.scheduler.event_queue.enqueue([&]
                {
                    if (!limit->hasSuspendedIncrease())
                        std::_Exit(2);
                    limit->notifyUnusedCapacityAvailable();
                    t.scheduler.removeChild(r.root_node.get());
                    r.root_node.reset();
                    limit_destroyed.set_value();
                    t.scheduler.event_queue.enqueue([&] { queue_drained.set_value(); });
                });
            });

            heavy->increaseAsync(5000);
            if (!heavy->waitPressureCountFor(1, std::chrono::seconds(5)))
                std::_Exit(3);
            if (limit_destroyed_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
                std::_Exit(4);
            if (queue_drained_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
                std::_Exit(5);
            std::_Exit(0);
        },
        ::testing::ExitedWithCode(0),
        "");
}


/// Removing the queue that owns the parked request releases its memory and must immediately retry
/// requests hidden in surviving siblings. The old pressure episode must not remain attached to the
/// detached owner.
TEST(SchedulerSpaceShared, DetachingParkedOwnerRetriesSurvivingSibling)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<FairAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    auto owner_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    owner_queue->basename = "owner";
    AllocationQueue * owner_queue_ptr = owner_queue.get();
    policy->attachChild(owner_queue);

    auto survivor_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, SchedulerNodeInfo{});
    survivor_queue->basename = "survivor";
    AllocationQueue * survivor_queue_ptr = survivor_queue.get();
    policy->attachChild(survivor_queue);

    r.root_node = limit;
    survivor_queue.reset();
    r.registerResource();

    auto heavy = std::make_unique<ManualAllocation>(owner_queue_ptr, "heavy", 8000);
    heavy->increaseAsync(5000);
    auto survivor = std::make_unique<ManualAllocation>(survivor_queue_ptr, "survivor", 3000, false);
    heavy->waitPressureCount(1);

    std::promise<void> detached;
    auto detached_future = detached.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        policy->removeChild(owner_queue.get());
        owner_queue.reset();
        detached.set_value();
    });
    detached_future.get();

    ASSERT_TRUE(survivor->waitSyncedFor(std::chrono::seconds(5)))
        << "Surviving sibling remained hidden after the parked owner detached";
    EXPECT_EQ(survivor->size(), 3000);
}


/// A suspended high-precedence child is a barrier only while it belongs to the policy. Detaching it
/// must erase the child before recomputing the barrier so runnable lower-precedence work can proceed.
TEST(SchedulerSpaceShared, DetachingSuspendedPrecedenceChildUnblocksLowerWork)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);

    auto limit = std::make_shared<AllocationLimit>(t.scheduler.event_queue, SchedulerNodeInfo{}, 10000);
    auto policy = std::make_shared<PrecedenceAllocation>(t.scheduler.event_queue, SchedulerNodeInfo{});
    policy->basename = "policy";
    limit->attachChild(policy);

    SchedulerNodeInfo high_info;
    high_info.setPrecedence(0);
    auto high_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, high_info);
    high_queue->basename = "high";
    AllocationQueue * high_queue_ptr = high_queue.get();
    policy->attachChild(high_queue);

    SchedulerNodeInfo low_info;
    low_info.setPrecedence(1);
    auto low_queue = std::make_shared<AllocationQueue>(t.scheduler.event_queue, low_info);
    low_queue->basename = "low";
    AllocationQueue * low_queue_ptr = low_queue.get();
    policy->attachChild(low_queue);

    r.root_node = limit;
    low_queue.reset();
    r.registerResource();

    auto heavy = std::make_unique<ManualAllocation>(high_queue_ptr, "heavy", 8000);
    heavy->increaseAsync(5000);
    auto lower = std::make_unique<ManualAllocation>(low_queue_ptr, "lower", 3000, false);
    heavy->waitPressureCount(1);

    std::promise<void> detached;
    auto detached_future = detached.get_future();
    t.scheduler.event_queue.enqueue([&]
    {
        policy->removeChild(high_queue.get());
        high_queue.reset();
        detached.set_value();
    });
    detached_future.get();

    ASSERT_TRUE(lower->waitSyncedFor(std::chrono::seconds(5)))
        << "Detached high-precedence suspension remained as a policy barrier";
    EXPECT_EQ(lower->size(), 3000);
}


/// A productive beneficiary is protected, not a reason to stop victim search. Suction must search
/// past it and choose another eligible allocation before considering the parked owner.
TEST(SchedulerSpaceShared, SuctionSearchesPastProtectedBeneficiary)
{
    SpaceSharedTest t;
    SpaceSharedResourceHolder r(t);
    r.addLimit("/", 10000);
    AllocationQueue * queue = r.addQueue("/queue");
    r.registerResource();

    ManualAllocation heavy(queue, "heavy", 3000);
    heavy.protectAfterPressureRounds(2);
    auto victim = std::make_unique<ManualAllocation>(queue, "victim", 1000);

    heavy.increaseAsync(8000);
    auto beneficiary = std::make_unique<ManualAllocation>(queue, "beneficiary", 5000, false);
    beneficiary->waitSynced();
    ASSERT_EQ(heavy.killCount(), 0u);
    ASSERT_EQ(beneficiary->killCount(), 0u);

    heavy.recoveryCheckpoint();
    ASSERT_TRUE(victim->waitKillsFor(1, std::chrono::seconds(5)))
        << "Victim search stopped at the protected beneficiary";
    EXPECT_EQ(beneficiary->killCount(), 0u);
    EXPECT_EQ(heavy.killCount(), 0u);
}


/// Zero is meaningful only after every runnable processor has supplied statistics for this
/// forced epoch. One zero observation cannot close the query-level episode before a later processor
/// reports real spillable memory.
TEST(SchedulerSpaceShared, ForcedSpillWaitsForAllRunnableProcessorStats)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor empty(0, /*spill_succeeds_=*/ false);
    ManualSpillProcessor spillable(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&empty);
    scheduler.registerProcessor(&spillable);
    scheduler.setProcessorRunnable(&empty, true);
    scheduler.setProcessorRunnable(&spillable, true);

    const auto request = scheduler.requestForcedSpill();
    scheduler.checkAndSpill(&empty);
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    scheduler.checkAndSpill(&spillable);
    EXPECT_EQ(spillable.spillCallCount(), 1u);
    EXPECT_NE(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);
}


/// Each runnable processor gets one observation in the epoch. Zero-byte processors do not pin the
/// backstop merely by remaining Ready, but the first zero observation cannot retire a different
/// runnable processor that has not supplied its own current statistics yet.
TEST(SchedulerSpaceShared, AllZeroRunnableProcessorsBoundForcedSpillEpoch)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor first(0, /*spill_succeeds_=*/ false);
    ManualSpillProcessor second(0, /*spill_succeeds_=*/ false);
    scheduler.registerProcessor(&first);
    scheduler.registerProcessor(&second);
    scheduler.setProcessorRunnable(&first, true);
    scheduler.setProcessorRunnable(&second, true);

    const auto request = scheduler.requestForcedSpill();
    EXPECT_TRUE(scheduler.checkAndSpill(&first));
    scheduler.beginForcedSpillScan();
    scheduler.setProcessorRunnable(&first, true);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    EXPECT_TRUE(scheduler.checkAndSpill(&second));
    scheduler.beginForcedSpillScan();
    scheduler.setProcessorRunnable(&second, true);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);
}


/// PipelineExecutor brackets recovery dispatch with this same task boundary. A zero observation is
/// not a timeout or a sync count; the transition to NoProgress occurs only when the complete stable
/// runnable census closes, even though recovery-only execution deliberately skips graph update.
TEST(SchedulerSpaceShared, RecoveryTaskBoundaryClosesAllZeroEpoch)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor empty(0, /*spill_succeeds_=*/ false);
    scheduler.registerProcessor(&empty);
    scheduler.setProcessorRunnable(&empty, true);

    scheduler.beginForcedSpillScan();
    const auto request = scheduler.requestForcedSpill();
    EXPECT_TRUE(scheduler.checkAndSpill(&empty));
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);
    scheduler.finishForcedSpillScan();

    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);
}


/// Graph registration is not evidence that a processor can execute recovery work. A permanently
/// dormant processor must not pin the epoch after the complete runnable pass proves that the only
/// executable processor has no spill candidate.
TEST(SchedulerSpaceShared, ForcedSpillIgnoresRegisteredButUnrunnableProcessor)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor dormant(4096, /*spill_succeeds_=*/ true);
    ManualSpillProcessor runnable(0, /*spill_succeeds_=*/ false);
    scheduler.registerProcessor(&dormant);
    scheduler.registerProcessor(&runnable);
    scheduler.setProcessorRunnable(&runnable, true);

    const auto request = scheduler.requestForcedSpill();
    scheduler.checkAndSpill(&runnable);
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    scheduler.beginForcedSpillScan();
    scheduler.setProcessorRunnable(&runnable, false);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);
    EXPECT_EQ(dormant.spillCallCount(), 0u);
}


/// Async readiness is a concrete graph event, not hypothetical future demand. Publishing it
/// inside the executor's scan boundary must keep the epoch Pending until the newly runnable task
/// receives its recovery-only turn.
TEST(SchedulerSpaceShared, AsyncReadyTransitionPublishesCandidateBeforeBoundaryCloses)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor async_candidate(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&async_candidate);

    scheduler.beginForcedSpillScan();
    const auto request = scheduler.requestForcedSpill();
    scheduler.setProcessorRunnable(&async_candidate, true);
    scheduler.finishForcedSpillScan();
    ASSERT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    scheduler.beginForcedSpillScan();
    EXPECT_TRUE(scheduler.checkAndSpill(&async_candidate));
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(async_candidate.spillCallCount(), 1u);
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);
}


/// A stable empty census may close immediately; it does not reserve capacity for imagined work.
/// If a real runnable spill candidate appears before suction is injected, that event earns exactly
/// one new recovery epoch. Repeated observations of the same runnable state do not earn another.
TEST(SchedulerSpaceShared, RunnableEventReopensNoProgressBeforeSuction)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor candidate(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&candidate);

    const auto empty_epoch = scheduler.requestForcedSpill();
    ASSERT_EQ(scheduler.getForcedSpillResult(empty_epoch.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);

    scheduler.beginForcedSpillScan();
    scheduler.setProcessorRunnable(&candidate, true);
    scheduler.finishForcedSpillScan();
    const auto reopened = scheduler.requestForcedSpill();
    ASSERT_GT(reopened.epoch, empty_epoch.epoch);
    ASSERT_FALSE(reopened.inject_priority);
    ASSERT_EQ(scheduler.getForcedSpillResult(reopened.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    scheduler.beginForcedSpillScan();
    EXPECT_TRUE(scheduler.checkAndSpill(&candidate));
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(candidate.spillCallCount(), 1u);
    EXPECT_EQ(scheduler.getForcedSpillResult(reopened.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);

    scheduler.setProcessorRunnable(&candidate, true);
    const auto suction = scheduler.requestForcedSpill();
    EXPECT_EQ(suction.epoch, reopened.epoch);
    EXPECT_TRUE(suction.inject_priority);
}


/// Runnable membership is query-global, while separate executors can update their graphs at the
/// same time. Finishing one complete scan must not use another scan's transient empty set as proof
/// that the query has no recovery candidate.
TEST(SchedulerSpaceShared, OverlappingForcedSpillScansCannotCloseEpochEarly)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor empty(0, /*spill_succeeds_=*/ false);
    scheduler.registerProcessor(&empty);
    scheduler.setProcessorRunnable(&empty, true);

    const auto request = scheduler.requestForcedSpill();
    scheduler.checkAndSpill(&empty);

    scheduler.beginForcedSpillScan();
    scheduler.beginForcedSpillScan();
    scheduler.setProcessorRunnable(&empty, false);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);
}


/// A concurrent ordinary graph task can enable a spillable downstream processor while a zero-byte
/// recovery task retires. The query-global task boundaries overlap, so the transient empty set is
/// not a no-candidate result and the new Ready processor claims the same epoch.
TEST(SchedulerSpaceShared, ConcurrentGraphUpdateAddsCandidateBeforeRecoveryBoundary)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor upstream(0, /*spill_succeeds_=*/ false);
    ManualSpillProcessor downstream(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&upstream);
    scheduler.registerProcessor(&downstream);
    scheduler.setProcessorRunnable(&upstream, true);

    scheduler.beginForcedSpillScan(); // Recovery-only task boundary.
    scheduler.beginForcedSpillScan(); // Concurrent ordinary execute/update boundary.
    const auto request = scheduler.requestForcedSpill();
    EXPECT_TRUE(scheduler.checkAndSpill(&upstream));
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    /// The ordinary task publishes its downstream transition before its outer boundary closes.
    scheduler.setProcessorRunnable(&upstream, false);
    scheduler.setProcessorRunnable(&downstream, true);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    scheduler.beginForcedSpillScan();
    scheduler.checkAndSpill(&downstream);
    scheduler.finishForcedSpillScan();
    EXPECT_EQ(downstream.spillCallCount(), 1u);
    EXPECT_EQ(scheduler.getForcedSpillResult(request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);
}


/// Completion is monotonic even though only the newest exact result is retained. A lagging worker
/// observing epoch N after N+1 completes must not see N become Pending again and wait forever.
TEST(SchedulerSpaceShared, LaggingObserverSeesOlderForcedSpillEpochComplete)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor processor(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&processor);
    scheduler.setProcessorRunnable(&processor, true);

    const auto first = scheduler.requestForcedSpill();
    scheduler.checkAndSpill(&processor);
    ASSERT_EQ(scheduler.getForcedSpillResult(first.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);
    scheduler.finishMemoryPressure();

    scheduler.setProcessorRunnable(&processor, false);
    const auto second = scheduler.requestForcedSpill();
    ASSERT_GT(second.epoch, first.epoch);
    ASSERT_EQ(scheduler.getForcedSpillResult(second.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::NoProgress);
    EXPECT_NE(scheduler.getForcedSpillResult(first.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);
}
