#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <optional>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <boost/intrusive/set.hpp>

#include <Common/CurrentMetrics.h>
#include <Common/ISlotControl.h>

namespace DB
{

/*
 * Controls how many threads can be allocated for a query (or another activity).
 * There is a limited amount of slots for threads. It can be set with `setMaxConcurrency(limit)`.
 *
 * Lifecycle of a slot: free -> granted -> acquired -> free.
 * free: slot is available to be allocated by any query.
 * granted: slot is allocated by specific query, but not yet acquired by any thread.
 * acquired: slot is allocated by specific query and acquired by a thread.
 *
 * Allocation model:
 *  - `allocate(min, max)` creates an allocation that gets at least `min` slots immediately.
 *    Under lazy allocation (default), one additional slot is granted at allocate time for
 *    bootstrap; further slots are granted one-at-a-time as previously-granted slots are
 *    acquired (via `tryAcquire`). This avoids reserving slots the consumer never uses.
 *  - `max` is mutable via `setMax(new_max)`: the consumer may raise the ceiling if the
 *    pipeline wants to upscale, or lower it to cap future grants. Growing past a saturated
 *    point re-inserts the allocation into the scheduler's waiter list.
 *
 * USAGE:
 *   1. Create an allocation:
 *      `auto slots = ConcurrencyControl::instance().allocate(min, max);`
 *   2. Acquire a slot per thread:
 *      `while (auto slot = slots->tryAcquire()) createYourThread([slot = std::move(slot)] { ... });`
 *   3. To raise or lower the ceiling at runtime: `slots->setMax(new_max);`
 * (all of these are non-blocking)
 *
 * There is a scheduler of CPU slots. It could be set with `setScheduler(name)`.
 * Possible values:
 *  - "round_robin":
 *    Released slots are distributed between waiting allocations in a round-robin manner to provide fairness.
 *    Oversubscription is possible: total amount of allocated slots can exceed `setMaxConcurrency(limit)`
 *    because `min` amount of slots is allocated for each query unconditionally.
 *  - "fair_round_robin":
 *    Also uses round-robin, but `min` slot are NOT holding real CPU slots.
 *    This way all `min` slots do not count into overall number of allocated slots. This leads to more fair competition.
 *    There is no oversubscription: total amount of allocated slots CANNOT exceed `setMaxConcurrency(limit)`.
 *  - "max_min_fair":
 *    Similar to "fair_round_robin", but released slots are always granted to the allocation with the minimum
 *    number of currently allocated slots. This provides better fairness under high oversubscription scenarios
 *    where many queries compete for limited CPU slots.
 */

class ConcurrencyControl;
struct ConcurrencyControlState
{
    ConcurrencyControlState();

    SlotCount available(std::unique_lock<std::mutex> &) const;

    mutable std::mutex mutex;

    // Slot counting
    SlotCount max_concurrency = UnlimitedSlots;
    SlotCount cur_concurrency = 0;
    CurrentMetrics::Increment max_concurrency_metric;

    // Rollback lever for the lazy allocation strategy. When false, pipelines bypass the
    // lazy `allocate(1, 1) + setMax` growth path and use eager `allocate(1, num_threads)`
    // directly (pre-#88339 behavior). Read without locking (relaxed).
    std::atomic<bool> lazy_allocation{true};

    // Total number of waiters across all three schedulers. Read lock-free by notifyAcquired()
    // as a fast-path: if no waiter exists anywhere there is nothing to schedule, so we skip
    // the state.mutex acquisition entirely. Writes happen under state.mutex; reads are relaxed.
    std::atomic<size_t> total_waiters{0};
};

class ConcurrencyControlRoundRobinScheduler
{
public:
    struct Allocation;
    using Waiters = std::list<Allocation *>;

    // Scoped guard for acquired slot, see Allocation::tryAcquire()
    struct Slot : public IAcquiredSlot
    {
        ~Slot() override;

    private:
        friend struct Allocation; // for ctor

        Slot(SlotAllocationPtr && allocation_, size_t slot_id_);

        SlotAllocationPtr allocation;
        CurrentMetrics::Increment acquired_slot_increment;
    };

    // Manages group of slots for a single query, see ConcurrencyControl::allocate(min, max)
    struct Allocation : public ISlotAllocation
    {
        ~Allocation() override;

        // Take one already granted slot if available. Lock-free iff there is no granted slot.
        [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

        // This is the same as tryAcquire(), waiting is not supported, so caller should only use it for the first `min` slots
        [[nodiscard]] AcquiredSlotPtr acquire() override;

        // Raise or lower the allocation's slot ceiling (see ISlotAllocation::setMax).
        void setMax(SlotCount new_max) override;

    private:
        friend struct Slot; // for release()
        friend class ConcurrencyControlRoundRobinScheduler; // for grant(), free(), schedule() and ctor

        Allocation(ConcurrencyControlRoundRobinScheduler & parent_, SlotCount limit_, SlotCount granted_, Waiters::iterator waiter_ = {});

        auto cancel()
        {
            std::unique_lock lock{mutex};
            return std::pair{allocated - released,
                allocated < limit ?
                    std::optional<Waiters::iterator>(waiter) :
                    std::optional<Waiters::iterator>()};
        }

        // Grant single slot to allocation, returns true iff more slot(s) are required
        bool grant();

        // Release one slot and grant it to other allocation if required
        void release();

        ConcurrencyControlRoundRobinScheduler & parent;

        mutable std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount limit; // mutable: raised or lowered via setMax
        SlotCount allocated; // allocated total (including already `released`)
        SlotCount released = 0;
        size_t last_slot_id = 0;

        std::atomic<SlotCount> granted; // allocated, but not yet acquired

        // Iterator to self in Waiters list; valid iff allocated < limit. Non-const so
        // setMax() can re-insert after a previously-saturated allocation gets grown.
        Waiters::iterator waiter;
    };

    ConcurrencyControlRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_);

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControlRoundRobinScheduler();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    SlotAllocationPtr allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max);

    // Round-robin scheduling of available slots among waiting allocations.
    // Bulk mode (lazy_grant=false): distributes ALL available capacity fairly (used by setMaxConcurrency).
    // Lazy multi (lazy_grant=true, single_grant=false): grant to waiters with granted==0; stops on first
    //   full round where no grant happens. Used by release/free/setMax to redistribute capacity.
    // Lazy single (lazy_grant=true, single_grant=true): grant at most 1 slot to 1 waiter. Used by
    //   notifyAcquired for one-at-a-time ramp-up.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    friend struct Allocation; // for free(), release(), notifyAcquired() and setMax internals

    void free(Allocation * allocation);
    void release(SlotCount amount);

    // Called by Allocation::tryAcquire() after a granted slot is consumed.
    // Triggers lazy schedule to grant the next slot for one-at-a-time ramp-up.
    void notifyAcquired();

    // Waiter list maintenance. Must be called under state.mutex.
    void addWaiterLocked(Allocation * allocation);   // inserts and maintains total_waiters
    void removeWaiterLocked(Allocation * allocation); // erases and maintains total_waiters

    ConcurrencyControl & parent;
    ConcurrencyControlState & state;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
};

class ConcurrencyControlFairRoundRobinScheduler
{
public:
    struct Allocation;
    using Waiters = std::list<Allocation *>;

    // Scoped guard for acquired slot, see Allocation::tryAcquire()
    struct Slot : public IAcquiredSlot
    {
        ~Slot() override;

    private:
        friend struct Allocation; // for ctor

        Slot(SlotAllocationPtr && allocation_, bool competing_, size_t slot_id_);

        SlotAllocationPtr allocation;
        bool competing; // true iff we count this slot in cur_conncurrency
        CurrentMetrics::Increment acquired_slot_increment;
    };

    // Manages group of slots for a single query, see ConcurrencyControl::allocate(min, max)
    struct Allocation : public ISlotAllocation
    {
        ~Allocation() override;

        // Take one already granted slot if available. Lock-free iff there is no granted slot.
        [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

        // This is the same as tryAcquire(), waiting is not supported, so caller should only use it for the first `min` slots
        [[nodiscard]] AcquiredSlotPtr acquire() override;

        // Raise or lower the allocation's slot ceiling (see ISlotAllocation::setMax).
        void setMax(SlotCount new_max) override;

    private:
        friend struct Slot; // for release()
        friend class ConcurrencyControlFairRoundRobinScheduler; // for grant(), free(), schedule() and ctor

        Allocation(ConcurrencyControlFairRoundRobinScheduler & parent_, SlotCount min_, SlotCount max, SlotCount granted_, Waiters::iterator waiter_ = {});

        auto cancel()
        {
            std::unique_lock lock{mutex};
            return std::pair{allocated - released,
                allocated < limit ?
                    std::optional<Waiters::iterator>(waiter) :
                    std::optional<Waiters::iterator>()};
        }

        // Grant single slot to allocation, returns true iff more slot(s) are required
        bool grant();

        // Release one slot and grant it to other allocation if required
        void release();

        ConcurrencyControlFairRoundRobinScheduler & parent;
        const SlotCount min;

        mutable std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount limit; // mutable: raised or lowered via setMax (competing slots only; excludes `min`)
        SlotCount allocated; // allocated total excluding non-competing (including already `released`)
        SlotCount released = 0;
        size_t last_slot_id = 0;

        std::atomic<SlotCount> noncompeting; // allocated noncompeting slots, but not yet acquired
        std::atomic<SlotCount> granted; // allocated competing slots, but not yet acquired

        // Iterator to self in Waiters list; valid iff allocated < limit. Non-const so setMax()
        // can re-insert after a previously-saturated allocation gets grown.
        Waiters::iterator waiter;
    };

    ConcurrencyControlFairRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_);

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControlFairRoundRobinScheduler();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    SlotAllocationPtr allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max);

    // See RoundRobinScheduler::schedule for semantics.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    friend struct Allocation; // for free(), release(), notifyAcquired() and setMax internals

    void free(Allocation * allocation);
    void release(SlotCount amount);
    void notifyAcquired();

    void addWaiterLocked(Allocation * allocation);
    void removeWaiterLocked(Allocation * allocation);

    ConcurrencyControl & parent;
    ConcurrencyControlState & state;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
};

class ConcurrencyControlMaxMinFairScheduler
{
public:
    // Forward declarations
    struct AllocationCompare;
    struct Slot;

    // Manages group of slots for a single query, see ConcurrencyControl::allocate(min, max)
    struct Allocation : public ISlotAllocation
    {
        ~Allocation() override;

        // Take one already granted slot if available. Lock-free iff there is no granted slot.
        [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

        // This is the same as tryAcquire(), waiting is not supported, so caller should only use it for the first `min` slots
        [[nodiscard]] AcquiredSlotPtr acquire() override;

        // Raise or lower the allocation's slot ceiling (see ISlotAllocation::setMax).
        void setMax(SlotCount new_max) override;

    private:
        friend struct Slot; // for release()
        friend class ConcurrencyControlMaxMinFairScheduler; // for grant(), free(), schedule() and ctor
        friend struct AllocationCompare;

        Allocation(ConcurrencyControlMaxMinFairScheduler & parent_, SlotCount min_, SlotCount max, SlotCount granted_, UInt64 sequence_number_);

        auto cancel()
        {
            std::unique_lock lock{mutex};
            return std::pair{allocated - released, waiters_hook.is_linked()};
        }

        // Grant single slot to allocation, returns true iff more slot(s) are required
        bool grant();

        // Release one slot and grant it to other allocation if required
        void release();

        ConcurrencyControlMaxMinFairScheduler & parent;
        const SlotCount min;

        mutable std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount limit; // mutable: raised or lowered via setMax (competing slots only)
        SlotCount allocated; // allocated total excluding non-competing (including already `released`)
        SlotCount released = 0;
        size_t last_slot_id = 0;

        std::atomic<SlotCount> noncompeting; // allocated noncompeting slots, but not yet acquired
        std::atomic<SlotCount> granted; // allocated competing slots, but not yet acquired

        UInt64 sequence_number; // monotonically increasing counter for FIFO ordering
        boost::intrusive::set_member_hook<> waiters_hook; // use is_linked() to check if waiting
    };

    // Scoped guard for acquired slot, see Allocation::tryAcquire()
    struct Slot : public IAcquiredSlot
    {
        ~Slot() override;

    private:
        friend struct Allocation; // for ctor

        Slot(SlotAllocationPtr && allocation_, bool competing_, size_t slot_id_);

        SlotAllocationPtr allocation;
        bool competing; // true iff we count this slot in cur_concurrency
        CurrentMetrics::Increment acquired_slot_increment;
    };

    // Use boost intrusive set sorted by (allocated, sequence_number) to efficiently find minimum allocation
    // sequence_number ensures FIFO order when allocated counts are equal
    struct AllocationCompare
    {
        bool operator()(const Allocation & lhs, const Allocation & rhs) const;
    };
    using Waiters = boost::intrusive::set<
        Allocation,
        boost::intrusive::compare<AllocationCompare>,
        boost::intrusive::member_hook<Allocation, boost::intrusive::set_member_hook<>, &Allocation::waiters_hook>
    >;

    ConcurrencyControlMaxMinFairScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_);

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControlMaxMinFairScheduler();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    SlotAllocationPtr allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max);

    // Max-min fair scheduling. See RoundRobinScheduler::schedule for mode semantics.
    // Lazy mode is work-conserving: iterates waiters in (allocated, sequence_number) order
    // and grants to the first with granted==0. Long-term max-min fairness is preserved by
    // the sort order (lowest-allocated eligible waiter wins); if all lower-allocated waiters
    // have pending grants, a higher-allocated one gets the slot instead of stalling.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    friend struct Allocation; // for free(), release(), notifyAcquired() and setMax internals

    void free(Allocation * allocation);
    void release(SlotCount amount);

    // Work-conserving lazy single grant: scans waiters in sort order and grants to the first
    // with granted==0. See schedule() for the fairness discussion.
    void notifyAcquired();

    void addWaiterLocked(Allocation * allocation);
    void removeWaiterLocked(Allocation * allocation);

    ConcurrencyControl & parent;
    ConcurrencyControlState & state;
    Waiters waiters;
    UInt64 next_sequence_number = 0; // monotonically increasing counter for FIFO ordering
};

class ConcurrencyControl : public ISlotControl
{
public:
    ConcurrencyControl();
    ~ConcurrencyControl() override = default;

    static ConcurrencyControl & instance();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    [[nodiscard]] SlotAllocationPtr allocate(SlotCount min, SlotCount max) override;

    // Sets value of the current slot limit
    void setMaxConcurrency(SlotCount value);

    // Sets the current scheduling algorithm. Returns true if `value` is valid
    bool setScheduler(const String & value);

    // Returns the current scheduling algorithm
    String getScheduler() const;

    // Rollback lever: when false, callers are expected to use eager `allocate(1, num_threads)`
    // bypassing the lazy `allocate(1, 1) + setMax` growth path. Default is true (lazy). Safe
    // to toggle at runtime — only affects new allocations.
    void setLazyAllocation(bool value);
    bool getLazyAllocation() const;

    // Schedule available slots to waiters.
    // Do not call directly, for internal use only.
    // In bulk mode (lazy=false): distributes all available capacity fairly.
    // In lazy mode (lazy=true): grants at most 1 slot to 1 waiter with granted==0.
    // lazy_grant: if true, skip waiters with granted > 0 (preserves fairness for release/notify paths).
    // single_grant: if true, stop after 1 grant (only checked when lazy_grant=true).
    // Default (lazy_grant=false, single_grant=false) is bulk mode — original eager distribution.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    ConcurrencyControlState state;

    enum class Scheduler : uint8_t { RoundRobin, FairRoundRobin, MaxMinFair };
    Scheduler scheduler = Scheduler::RoundRobin;
    ConcurrencyControlRoundRobinScheduler round_robin;
    ConcurrencyControlFairRoundRobinScheduler fair_round_robin;
    ConcurrencyControlMaxMinFairScheduler max_min_fair;
};

}
