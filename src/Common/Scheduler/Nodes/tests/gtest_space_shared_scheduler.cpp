#include <gtest/gtest.h>

#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/FairAllocation.h>
#include <Common/Scheduler/Nodes/SpaceShared/PrecedenceAllocation.h>
#include <Common/Scheduler/Nodes/tests/ResourceTest.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/MemoryTracker.h>
#include <Processors/IProcessor.h>

#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <future>
#include <functional>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
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
    ManualAllocation(AllocationQueue * queue_, const String & name_, ResourceCost initial_size, bool wait_for_admission = true)
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
        queue.notifyRecoveryProgress(*this);
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

    void increaseCancelled() override
    {
        std::unique_lock lock(mutex);
        increase_enqueued = false;
        cv.notify_all();
    }

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
    size_t protection_round = 1;
    size_t current_pressure_round = 0;
    size_t total_pressure_events = 0;
    bool recovery_active = false;
    std::function<void()> next_pressure_callback;
    std::optional<ResourceCost> reconciled_increase_size;
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


/// Worker wakeups and memory-sync calls cannot advance pressure. Suction becomes eligible only
/// after the selected processor explicitly finishes the forced spill attempt.
TEST(SchedulerSpaceShared, ExplicitSpillCompletionControlsSuctionPriority)
{
    MemorySpillScheduler scheduler(/*enable_=*/ false);
    ManualSpillProcessor processor(4096, /*spill_succeeds_=*/ true);
    scheduler.registerProcessor(&processor);

    const auto first_request = scheduler.requestForcedSpill();
    ASSERT_GT(first_request.epoch, 0u);
    EXPECT_FALSE(first_request.inject_priority);
    EXPECT_EQ(scheduler.getForcedSpillResult(first_request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);

    /// Re-observing the same blocked request before reclaim completion must not inject priority.
    const auto repeated_request = scheduler.requestForcedSpill();
    EXPECT_EQ(repeated_request.epoch, first_request.epoch);
    EXPECT_FALSE(repeated_request.inject_priority);

    scheduler.checkAndSpill(&processor);
    EXPECT_EQ(processor.spillCallCount(), 1u);
    EXPECT_EQ(processor.lastSpillSize(), 4096u);
    EXPECT_EQ(scheduler.getForcedSpillResult(first_request.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Progress);

    const auto suction_request = scheduler.requestForcedSpill();
    EXPECT_EQ(suction_request.epoch, first_request.epoch);
    EXPECT_TRUE(suction_request.inject_priority);
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

    const auto first = scheduler.requestForcedSpill();
    scheduler.checkAndSpill(&previously_selected);
    ASSERT_NE(
        scheduler.getForcedSpillResult(first.epoch).outcome,
        MemorySpillScheduler::ForcedSpillOutcome::Pending);
    scheduler.finishMemoryPressure();

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
