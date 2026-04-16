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
 * Invariants (enforced by all three schedulers):
 *  - `waiters`: set/list of allocations that have `allocated < limit` and may receive more slots.
 *  - `demanders` ⊆ `waiters`: the subset with `has_more_demand == true`. Lazy scheduling
 *    iterates `demanders` — not `waiters` — so the hot path scales with actively-demanding
 *    queries, not the full waiter list.
 *  - `demanders` membership flips via `setMoreDemand`. An allocation enters `demanders` at
 *    creation (default demand=true) and leaves on `setMoreDemand(false)`, on losing waiter
 *    status (allocated reached limit), or on destruction. It re-enters on `setMoreDemand(true)`.
 *  - All membership changes (add/remove from `waiters`, `demanders`) happen under `state.mutex`.
 *  - MMF uses `allocated` as the sort key of both intrusive sets — `grant()` must not run while
 *    the allocation is linked in either set (remove → grant → re-insert).
 *  - `setMoreDemand` MUST NOT be called while holding `state.mutex` — it takes that lock itself.
 *
 * USAGE:
 *   1. Create an allocation for a query:
 *      `auto slots = ConcurrencyControl::instance().allocate(min, max);`
 *      It will allocate at least `min` and at most `max` slots.
 *      Note that `min` slots are granted immediately, but other `max - min` may be granted later.
 *   2. For every thread a slot has to be acquired from that allocation:
 *      `while (auto slot = slots->tryAcquire()) createYourThread([slot = std::move(slot)] { ... });`
 *      This snippet can be used at query startup and for upscaling later.
 * (both functions are non-blocking)
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

    // Emergency revert for the lazy allocation strategy. When false, allocate() grants all
    // slots that fit in remaining capacity (pre-#88339 behavior). When true (default), only
    // `min + at most 1` slots are granted, with the rest granted lazily via setMoreDemand.
    // Intended as a rollback lever in case the lazy strategy regresses on a specific workload.
    // Read without locking (relaxed) — toggling mid-query is racy by design (only affects
    // allocations made after the toggle).
    std::atomic<bool> lazy_allocation{true};

    // Total number of demanders across all three schedulers (maintained by addDemanderLocked/
    // removeDemanderLocked). Read lock-free by notifyAcquired() as a fast-path: if no
    // demanders exist anywhere, there is nothing to schedule and we skip the state.mutex
    // acquisition entirely. Writes happen under state.mutex; reads are relaxed.
    std::atomic<size_t> total_demanders{0};
};

class ConcurrencyControlRoundRobinScheduler
{
public:
    struct Allocation;
    using Waiters = std::list<Allocation *>;
    // `demanders` is a subset of `waiters` containing only those with has_more_demand == true.
    // Lazy schedule iterates this list (not the full waiters list) so the cost scales with
    // the number of actively demanding allocations, not the total number of waiters.
    using Demanders = std::list<Allocation *>;

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

        // Signal whether the consumer currently wants more slots granted (see ISlotAllocation).
        void setMoreDemand(bool value) override;

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
        const SlotCount limit;

        mutable std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount allocated; // allocated total (including already `released`)
        SlotCount released = 0;
        size_t last_slot_id = 0;

        std::atomic<SlotCount> granted; // allocated, but not yet acquired
        std::atomic<bool> has_more_demand{true}; // consumer wants more slots granted (see setMoreDemand)

        // Iterator to self in Waiters list; valid iff allocated < limit.
        // Non-const so reclaim() can re-insert after a previously-saturated allocation gets
        // its budget restored via setMoreDemand(false)/reclaim.
        Waiters::iterator waiter;
        // Iterator to self in Demanders list; valid iff `in_demanders` is true.
        Demanders::iterator demander_iter;
        bool in_demanders = false; // whether this allocation is currently in the demanders list
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
    // In bulk mode (lazy=false): distributes ALL available capacity fairly. Used by release/setMaxConcurrency.
    // In lazy mode (lazy=true): grants at most 1 slot to 1 waiter with granted==0. Used by notifyAcquired.
    // lazy_grant: if true, skip waiters with granted > 0 (preserves fairness for release/notify paths).
    // single_grant: if true, stop after 1 grant (only checked when lazy_grant=true).
    // Default (lazy_grant=false, single_grant=false) is bulk mode — original eager distribution.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    friend struct Allocation; // for free(), release(), notifyAcquired(), notifyDemand() and reclaim()

    void free(Allocation * allocation);
    void release(SlotCount amount);

    // Called by Allocation::tryAcquire() after a granted slot is consumed.
    // Triggers lazy schedule to grant the next slot for one-at-a-time ramp-up.
    void notifyAcquired();

    // Called by Allocation::setMoreDemand() on false->true transition.
    // Triggers lazy schedule so allocations that gained demand can be granted slots.
    void notifyDemand();

    // Called by Allocation::setMoreDemand(false) to return pending (granted but not acquired) slots.
    // Atomically decrements cur_concurrency and updates the allocation's accounting.
    void reclaim(Allocation * allocation, SlotCount amount);

    // Demanders list maintenance. These require state.mutex. Invariant: demanders ⊆ waiters AND
    // every allocation in demanders has has_more_demand == true.
    void addDemanderLocked(Allocation * allocation);   // caller holds state.mutex
    void removeDemanderLocked(Allocation * allocation); // caller holds state.mutex

    ConcurrencyControl & parent;
    ConcurrencyControlState & state;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
    Demanders demanders; // subset of waiters with has_more_demand == true
    Demanders::iterator cur_demander; // round-robin pointer for demanders
};

class ConcurrencyControlFairRoundRobinScheduler
{
public:
    struct Allocation;
    using Waiters = std::list<Allocation *>;
    // See RoundRobinScheduler::Demanders.
    using Demanders = std::list<Allocation *>;

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

        // Signal whether the consumer currently wants more slots granted (see ISlotAllocation).
        void setMoreDemand(bool value) override;

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
        const SlotCount limit;

        mutable std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount allocated; // allocated total excluding non-competing (including already `released`)
        SlotCount released = 0;
        size_t last_slot_id = 0;

        std::atomic<SlotCount> noncompeting; // allocated noncompeting slots, but not yet acquired
        std::atomic<SlotCount> granted; // allocated competing slots, but not yet acquired
        std::atomic<bool> has_more_demand{true}; // consumer wants more slots granted (see setMoreDemand)

        // Iterator to self in Waiters list; valid iff allocated < limit.
        // Non-const so reclaim() can re-insert after a previously-saturated allocation.
        Waiters::iterator waiter;
        Demanders::iterator demander_iter; // valid iff `in_demanders` is true
        bool in_demanders = false;
    };

    ConcurrencyControlFairRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_);

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControlFairRoundRobinScheduler();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    SlotAllocationPtr allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max);

    // Round-robin scheduling of available slots among waiting allocations.
    // Bulk mode (lazy=false) and lazy mode (lazy=true) — see RoundRobinScheduler for semantics.
    // lazy_grant: if true, skip waiters with granted > 0 (preserves fairness for release/notify paths).
    // single_grant: if true, stop after 1 grant (only checked when lazy_grant=true).
    // Default (lazy_grant=false, single_grant=false) is bulk mode — original eager distribution.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    friend struct Allocation; // for free(), release(), notifyAcquired(), notifyDemand() and reclaim()

    void free(Allocation * allocation);
    void release(SlotCount amount);
    void notifyAcquired();
    void notifyDemand();
    void reclaim(Allocation * allocation, SlotCount amount);

    void addDemanderLocked(Allocation * allocation);
    void removeDemanderLocked(Allocation * allocation);

    ConcurrencyControl & parent;
    ConcurrencyControlState & state;
    Waiters waiters;
    Waiters::iterator cur_waiter; // round-robin pointer
    Demanders demanders;
    Demanders::iterator cur_demander;
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

        // Signal whether the consumer currently wants more slots granted (see ISlotAllocation).
        void setMoreDemand(bool value) override;

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
        const SlotCount limit;

        mutable std::mutex mutex; // the following values must be accessed under this mutex
        SlotCount allocated; // allocated total excluding non-competing (including already `released`)
        SlotCount released = 0;
        size_t last_slot_id = 0;

        std::atomic<SlotCount> noncompeting; // allocated noncompeting slots, but not yet acquired
        std::atomic<SlotCount> granted; // allocated competing slots, but not yet acquired
        std::atomic<bool> has_more_demand{true}; // consumer wants more slots granted (see setMoreDemand)

        UInt64 sequence_number; // monotonically increasing counter for FIFO ordering
        boost::intrusive::set_member_hook<> waiters_hook; // intrusive hook for waiters set, use is_linked() to check if waiting
        // Separate hook for the demanders set (subset of waiters with has_more_demand == true).
        // See RoundRobinScheduler::Demanders.
        boost::intrusive::set_member_hook<> demanders_hook;
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
    // Demanders is the same sort order as Waiters but contains only allocations with
    // has_more_demand == true. Lazy schedule iterates this set so the hot path scales with the
    // number of actively demanding allocations, not the total number of waiters.
    using Demanders = boost::intrusive::set<
        Allocation,
        boost::intrusive::compare<AllocationCompare>,
        boost::intrusive::member_hook<Allocation, boost::intrusive::set_member_hook<>, &Allocation::demanders_hook>
    >;

    ConcurrencyControlMaxMinFairScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_);

    // WARNING: all Allocation objects MUST be destructed before ConcurrencyControl
    // NOTE: Recommended way to achieve this is to use `instance()` and do graceful shutdown of queries
    ~ConcurrencyControlMaxMinFairScheduler();

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a subscription for later allocation is created
    // Use `Allocation::tryAcquire()` to acquire allocated slot, before running a thread.
    SlotAllocationPtr allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max);

    // Max-min fair scheduling of available slots among waiting allocations.
    // Bulk mode (lazy=false) and lazy mode (lazy=true) — see RoundRobinScheduler for semantics.
    // lazy_grant: if true, skip waiters with granted > 0 (preserves fairness for release/notify paths).
    // single_grant: if true, stop after 1 grant (only checked when lazy_grant=true).
    // Default (lazy_grant=false, single_grant=false) is bulk mode — original eager distribution.
    void schedule(std::unique_lock<std::mutex> &, bool lazy_grant = false, bool single_grant = false);

private:
    friend struct Allocation; // for free(), release() and notifyAcquired()

    void free(Allocation * allocation);
    void release(SlotCount amount);

    // Min-level lazy single grant: finds a waiter at the minimum allocation level with granted==0
    // and demand, and grants 1 slot. Does not descend to higher-allocated waiters, preserving max-min fairness.
    void notifyAcquired();

    // Called on false->true demand transition — triggers lazy schedule.
    void notifyDemand();

    // Return pending slots to the pool on setMoreDemand(false).
    void reclaim(Allocation * allocation, SlotCount amount);

    // Demanders set maintenance (see RoundRobinScheduler). Caller holds state.mutex.
    void addDemanderLocked(Allocation * allocation);
    void removeDemanderLocked(Allocation * allocation);

    ConcurrencyControl & parent;
    ConcurrencyControlState & state;
    Waiters waiters;
    Demanders demanders;
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

    // Emergency revert lever: when false, allocate() grants everything eagerly (pre-#88339).
    // Default is true (lazy). Safe to toggle at runtime — only affects new allocations.
    void setLazyAllocation(bool value);

    // Returns the current value of the lazy allocation flag.
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
