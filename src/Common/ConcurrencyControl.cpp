#include <Common/ISlotControl.h>
#include <Common/ConcurrencyControl.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event ConcurrencyControlSlotsGranted;
    extern const Event ConcurrencyControlSlotsDelayed;
    extern const Event ConcurrencyControlSlotsAcquired;
    extern const Event ConcurrencyControlSlotsAcquiredNonCompeting;
    extern const Event ConcurrencyControlQueriesDelayed;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlAcquiredNonCompeting;
    extern const Metric ConcurrencyControlSoftLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ConcurrencyControlState::ConcurrencyControlState()
    : max_concurrency_metric(CurrentMetrics::ConcurrencyControlSoftLimit, 0)
{
}

SlotCount ConcurrencyControlState::available(std::unique_lock<std::mutex> &) const
{
    if (cur_concurrency < max_concurrency)
        return max_concurrency - cur_concurrency;
    return 0;
}


ConcurrencyControlRoundRobinScheduler::Slot::Slot(SlotAllocationPtr && allocation_, size_t slot_id_)
    : IAcquiredSlot(slot_id_)
    , allocation(std::move(allocation_))
    , acquired_slot_increment(CurrentMetrics::ConcurrencyControlAcquired)
{
}

ConcurrencyControlRoundRobinScheduler::Slot::~Slot()
{
    static_cast<ConcurrencyControlRoundRobinScheduler::Allocation&>(*allocation).release();
}

ConcurrencyControlRoundRobinScheduler::Allocation::Allocation(ConcurrencyControlRoundRobinScheduler & parent_, SlotCount limit_, SlotCount granted_, Waiters::iterator waiter_)
    : parent(parent_)
    , limit(limit_)
    , allocated(granted_)
    , granted(granted_)
    , waiter(waiter_)
{
    if (allocated < limit)
        *waiter = this;
}

ConcurrencyControlRoundRobinScheduler::Allocation::~Allocation()
{
    // We have to lock parent's mutex to avoid race with grant()
    // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
    parent.free(this);
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlRoundRobinScheduler::Allocation::tryAcquire()
{
    SlotCount value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            AcquiredSlotPtr result;
            {
                std::unique_lock lock{mutex};
                result = AcquiredSlotPtr(new Slot(shared_from_this(), last_slot_id++)); // can't use std::make_shared due to private ctor
            }
            // Trigger lazy scheduling: a granted slot was consumed, so the scheduler
            // may grant the next one. Must be called after releasing allocation mutex
            // to respect lock ordering (state.mutex -> allocation.mutex).
            parent.notifyAcquired();
            return result;
        }
    }
    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlRoundRobinScheduler::Allocation::acquire()
{
    auto result = tryAcquire();
    chassert(result);
    return result;
}

// Grant single slot to allocation returns true iff more slot(s) are required
bool ConcurrencyControlRoundRobinScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

// Release one slot and grant it to other allocation if required
void ConcurrencyControlRoundRobinScheduler::Allocation::release()
{
    parent.release(1);
    std::unique_lock lock{mutex};
    released++;
    if (released > allocated)
        abort();
}

ConcurrencyControlRoundRobinScheduler::ConcurrencyControlRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_)
    : parent(parent_)
    , state(state_)
    , cur_waiter(waiters.end())
    , cur_demander(demanders.end())
{
}

ConcurrencyControlRoundRobinScheduler::~ConcurrencyControlRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

void ConcurrencyControlRoundRobinScheduler::addDemanderLocked(Allocation * allocation)
{
    if (allocation->in_demanders)
        return;
    allocation->demander_iter = demanders.insert(cur_demander, allocation);
    allocation->in_demanders = true;
    state.total_demanders.fetch_add(1, std::memory_order_relaxed);
}

void ConcurrencyControlRoundRobinScheduler::removeDemanderLocked(Allocation * allocation)
{
    if (!allocation->in_demanders)
        return;
    if (cur_demander == allocation->demander_iter)
        cur_demander = demanders.erase(allocation->demander_iter);
    else
        demanders.erase(allocation->demander_iter);
    allocation->in_demanders = false;
    state.total_demanders.fetch_sub(1, std::memory_order_relaxed);
}

SlotAllocationPtr ConcurrencyControlRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Lazy allocation: grant `min` slots unconditionally (oversubscription allowed),
    // plus at most 1 additional slot from available capacity for ramp-up bootstrap.
    // The +1 only fires if there's real capacity beyond the min oversubscription,
    // otherwise the min slots already consume all (or more than all) available capacity.
    // Remaining slots are granted one-at-a-time via lazy schedule() as they are actually acquired.
    //
    // Emergency revert (state.lazy_allocation=false): restore pre-#88339 eager behavior —
    // grant up to `max` slots immediately from available capacity.
    SlotCount granted;
    if (state.lazy_allocation.load(std::memory_order_relaxed))
    {
        granted = min;
        if (granted < max && state.available(lock) > min)
            granted += 1;
    }
    else
    {
        granted = std::max(min, std::min(max, state.available(lock)));
    }
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation and start waiting if more slots are required
    if (granted < max)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, max - granted);
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        auto alloc = SlotAllocationPtr(new Allocation(*this, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
        // New allocation defaults to has_more_demand == true; register it in demanders.
        addDemanderLocked(static_cast<Allocation *>(alloc.get()));
        return alloc;
    }
    else
    {
        return SlotAllocationPtr(new Allocation(*this, max, granted));
    }
}

void ConcurrencyControlRoundRobinScheduler::free(Allocation * allocation)
{
    // Allocation is allowed to be canceled even if there are:
    //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
    //  - `waiter`: active waiting for more slots to be allocated
    // Thus Allocation destruction may require the following lock, to avoid race conditions
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    removeDemanderLocked(allocation);
    if (waiter)
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// Round-robin scheduling of available slots among waiting allocations.
// Bulk mode (lazy_grant=false): distributes ALL available capacity fairly (setMaxConcurrency).
// Lazy multi (lazy_grant=true, single_grant=false): skip waiters with pending grants OR no demand,
//   grant multiple (used by release/free to redistribute freed capacity).
// Lazy single (lazy_grant=true, single_grant=true): skip pending or no-demand, grant at most 1
//   (used by notifyAcquired for demand-driven ramp-up).
void ConcurrencyControlRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &, bool lazy_grant, bool single_grant)
{
    if (lazy_grant)
    {
        // Iterate the `demanders` list — a subset of `waiters` containing only allocations with
        // has_more_demand == true. This makes the lazy hot path scale with the number of actively
        // demanding queries, not the total number of waiters (addresses reviewer perf concern).
        //
        // TODO: when a demander sits with granted > 0 for a long time (pipeline acquired but
        // hasn't called tryAcquire again), every release/notifyAcquired round walks past it
        // before finding a grantable demander. Worst case is O(demanders) per acquire under
        // persistent stuck-demander. If this becomes a hot path, split into "ready" (granted==0)
        // and "pending" (granted>0) sub-lists so only the ready list is scanned.
        size_t skipped = 0;
        while (!demanders.empty() && state.cur_concurrency < state.max_concurrency)
        {
            if (cur_demander == demanders.end())
                cur_demander = demanders.begin();
            Allocation * allocation = *cur_demander;

            // Skip only on pending grants; no-demand allocations aren't in this list at all.
            if (allocation->granted.load(std::memory_order_relaxed) > 0)
            {
                ++cur_demander;
                if (++skipped > demanders.size())
                    break; // Every demander has a pending grant
                continue;
            }

            skipped = 0;
            state.cur_concurrency++;
            bool still_waiter = allocation->grant();
            ++cur_demander;
            if (!still_waiter)
            {
                // Allocation reached its limit — remove from both demanders and waiters lists.
                removeDemanderLocked(allocation);
                if (cur_waiter == allocation->waiter)
                    cur_waiter = waiters.erase(allocation->waiter);
                else
                    waiters.erase(allocation->waiter);
            }
            if (single_grant)
                return;
        }
    }
    else
    {
        // Bulk mode: original eager distribution. Iterates the full waiters list.
        while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
        {
            state.cur_concurrency++;
            if (cur_waiter == waiters.end())
                cur_waiter = waiters.begin();
            Allocation * allocation = *cur_waiter;
            bool still_waiter = allocation->grant();
            if (still_waiter)
            {
                ++cur_waiter;
            }
            else
            {
                // Remove from demanders too (if present) since no longer a waiter.
                removeDemanderLocked(allocation);
                cur_waiter = waiters.erase(cur_waiter);
            }
        }
    }
}

void ConcurrencyControlRoundRobinScheduler::notifyAcquired()
{
    // Fast path: if no demanders anywhere, no one to grant to — skip the state.mutex acquisition.
    // A racing addDemanderLocked ordering is fine: we'd just miss one schedule round, and the
    // next notifyAcquired (another tryAcquire) or notifyDemand would pick it up.
    if (state.total_demanders.load(std::memory_order_relaxed) == 0)
        return;
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ true);
}

void ConcurrencyControlRoundRobinScheduler::notifyDemand()
{
    std::unique_lock lock{state.mutex};
    // Lazy multi: a waiter just gained demand; try to grant slots (to any waiter with demand and no pending)
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// Return pending (granted but not acquired) slots to the pool. Called from setMoreDemand(false).
// Atomically decrements cur_concurrency, removes the allocation from demanders, and restores
// the allocation's grant budget by decrementing `allocated` — reclaimed slots must not count
// against the allocation's lifetime limit, otherwise repeated demand flapping would ratchet down
// the query's attainable parallelism.
//
// If the allocation was saturated (allocated == limit → not in waiters), re-insert into waiters
// so future schedule rounds can grant to it again. The demanders list is NOT restored because
// reclaim is triggered by setMoreDemand(false); a subsequent setMoreDemand(true) will re-add.
void ConcurrencyControlRoundRobinScheduler::reclaim(Allocation * allocation, SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    removeDemanderLocked(allocation);

    {
        // Hold allocation.mutex across the allocated decrement AND the waiter iterator
        // assignment: cancel() (invoked later from ~Allocation/free) also reads these fields
        // under allocation.mutex, so both must be serialized. waiters.insert operates on
        // state.mutex-protected data, which we hold; interleaving alock under lock is fine.
        std::unique_lock alock{allocation->mutex};
        bool was_waiter = allocation->allocated < allocation->limit;
        allocation->allocated -= amount;

        if (!was_waiter && allocation->allocated < allocation->limit)
            allocation->waiter = waiters.insert(cur_waiter, allocation);
    }

    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// MUST NOT be called while holding state.mutex — both branches take it internally.
// Repeat calls with the same value are idempotent (the exchange returns the same value).
void ConcurrencyControlRoundRobinScheduler::Allocation::setMoreDemand(bool value)
{
    bool prev = has_more_demand.exchange(value, std::memory_order_acq_rel);
    if (!value && prev)
    {
        // true -> false: reclaim any pending (granted but not acquired) slots so they don't
        // sit idle in cur_concurrency blocking other queries. reclaim() also removes the
        // allocation from the demanders list. If there's nothing to reclaim, we still need
        // to remove from demanders so the lazy schedule skips this allocation.
        SlotCount to_reclaim = granted.exchange(0, std::memory_order_acq_rel);
        if (to_reclaim > 0)
        {
            parent.reclaim(this, to_reclaim);
        }
        else
        {
            std::unique_lock lock{parent.state.mutex};
            parent.removeDemanderLocked(this);
        }
    }
    else if (value && !prev)
    {
        // false -> true: add to demanders (only if still a waiter) and trigger scheduling.
        // The demanders ⊆ waiters invariant forbids adding a non-waiter allocation; that
        // happens if the allocation got all its slots at allocate time (not inserted into
        // waiters) or reached its limit mid-life.
        std::unique_lock lock{parent.state.mutex};
        if (allocated < limit)
            parent.addDemanderLocked(this);
        parent.parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
    }
}


ConcurrencyControlFairRoundRobinScheduler::Slot::Slot(SlotAllocationPtr && allocation_, bool competing_, size_t slot_id_)
    : IAcquiredSlot(slot_id_)
    , allocation(std::move(allocation_))
    , competing(competing_)
    , acquired_slot_increment(competing ? CurrentMetrics::ConcurrencyControlAcquired : CurrentMetrics::ConcurrencyControlAcquiredNonCompeting)
{
}

ConcurrencyControlFairRoundRobinScheduler::Slot::~Slot()
{
    if (competing)
        static_cast<ConcurrencyControlFairRoundRobinScheduler::Allocation&>(*allocation).release();
}

ConcurrencyControlFairRoundRobinScheduler::Allocation::Allocation(ConcurrencyControlFairRoundRobinScheduler & parent_, SlotCount min_, SlotCount max, SlotCount granted_, Waiters::iterator waiter_)
    : parent(parent_)
    , min(min_)
    , limit(max - min)
    , allocated(granted_)
    , noncompeting(min)
    , granted(granted_)
    , waiter(waiter_)
{
    if (allocated < limit)
        *waiter = this;
}

ConcurrencyControlFairRoundRobinScheduler::Allocation::~Allocation()
{
    // We have to lock parent's mutex to avoid race with grant()
    // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
    parent.free(this);
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlFairRoundRobinScheduler::Allocation::tryAcquire()
{
    // First try acquire non-competing slot (if any)
    SlotCount value = noncompeting.load();
    while (value)
    {
        if (noncompeting.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquiredNonCompeting, 1);
            AcquiredSlotPtr result;
            {
                std::unique_lock lock{mutex};
                result = AcquiredSlotPtr(new Slot(shared_from_this(), false, last_slot_id++)); // can't use std::make_shared due to private ctor
            }
            // Trigger lazy schedule so the first competing slot can be granted
            parent.notifyAcquired();
            return result;
        }
    }

    // If all non-competing slots are already acquired - try acquire granted (competing) slot
    value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            AcquiredSlotPtr result;
            {
                std::unique_lock lock{mutex};
                result = AcquiredSlotPtr(new Slot(shared_from_this(), true, last_slot_id++)); // can't use std::make_shared due to private ctor
            }
            parent.notifyAcquired();
            return result;
        }
    }

    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlFairRoundRobinScheduler::Allocation::acquire()
{
    auto result = tryAcquire();
    chassert(result);
    return result;
}

// Grant single slot to allocation returns true iff more slot(s) are required
bool ConcurrencyControlFairRoundRobinScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

// Release one slot and grant it to other allocation if required
void ConcurrencyControlFairRoundRobinScheduler::Allocation::release()
{
    parent.release(1);
    std::unique_lock lock{mutex};
    released++;
    if (released > allocated)
        abort();
}

ConcurrencyControlFairRoundRobinScheduler::ConcurrencyControlFairRoundRobinScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_)
    : parent(parent_)
    , state(state_)
    , cur_waiter(waiters.end())
    , cur_demander(demanders.end())
{
}

void ConcurrencyControlFairRoundRobinScheduler::addDemanderLocked(Allocation * allocation)
{
    if (allocation->in_demanders)
        return;
    allocation->demander_iter = demanders.insert(cur_demander, allocation);
    allocation->in_demanders = true;
    state.total_demanders.fetch_add(1, std::memory_order_relaxed);
}

void ConcurrencyControlFairRoundRobinScheduler::removeDemanderLocked(Allocation * allocation)
{
    if (!allocation->in_demanders)
        return;
    if (cur_demander == allocation->demander_iter)
        cur_demander = demanders.erase(allocation->demander_iter);
    else
        demanders.erase(allocation->demander_iter);
    allocation->in_demanders = false;
    state.total_demanders.fetch_sub(1, std::memory_order_relaxed);
}

ConcurrencyControlFairRoundRobinScheduler::~ConcurrencyControlFairRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

SlotAllocationPtr ConcurrencyControlFairRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Do not count `min` slots towards the limit. They are NOT considered as taking part in competition.
    // Lazy allocation: grant at most 1 competing slot from available capacity for ramp-up bootstrap.
    // Eager path (state.lazy_allocation=false) restores the pre-#88339 behavior — used as an
    // emergency rollback lever only.
    SlotCount limit = max - min;
    SlotCount granted = state.lazy_allocation.load(std::memory_order_relaxed)
        ? std::min(std::min(SlotCount(1), limit), state.available(lock))
        : std::min(limit, state.available(lock));
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation and start waiting if more slots are required
    if (granted < limit)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, limit - granted);
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        auto alloc = SlotAllocationPtr(new Allocation(*this, min, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
        addDemanderLocked(static_cast<Allocation *>(alloc.get()));
        return alloc;
    }
    else
    {
        return SlotAllocationPtr(new Allocation(*this, min, max, granted));
    }
}

void ConcurrencyControlFairRoundRobinScheduler::free(Allocation * allocation)
{
    // Allocation is allowed to be canceled even if there are:
    //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
    //  - `waiter`: active waiting for more slots to be allocated
    // Thus Allocation destruction may require the following lock, to avoid race conditions
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    removeDemanderLocked(allocation);
    if (waiter)
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlFairRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// Round-robin scheduling — tri-mode (see RoundRobinScheduler for semantics)
void ConcurrencyControlFairRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &, bool lazy_grant, bool single_grant)
{
    if (lazy_grant)
    {
        // Iterate the `demanders` subset rather than full `waiters` — O(demanders), not O(waiters).
        size_t skipped = 0;
        while (!demanders.empty() && state.cur_concurrency < state.max_concurrency)
        {
            if (cur_demander == demanders.end())
                cur_demander = demanders.begin();
            Allocation * allocation = *cur_demander;

            if (allocation->granted.load(std::memory_order_relaxed) > 0)
            {
                ++cur_demander;
                if (++skipped > demanders.size())
                    break;
                continue;
            }

            skipped = 0;
            state.cur_concurrency++;
            bool still_waiter = allocation->grant();
            ++cur_demander;
            if (!still_waiter)
            {
                removeDemanderLocked(allocation);
                if (cur_waiter == allocation->waiter)
                    cur_waiter = waiters.erase(allocation->waiter);
                else
                    waiters.erase(allocation->waiter);
            }
            if (single_grant)
                return;
        }
    }
    else
    {
        while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
        {
            state.cur_concurrency++;
            if (cur_waiter == waiters.end())
                cur_waiter = waiters.begin();
            Allocation * allocation = *cur_waiter;
            bool still_waiter = allocation->grant();
            if (still_waiter)
            {
                ++cur_waiter;
            }
            else
            {
                removeDemanderLocked(allocation);
                cur_waiter = waiters.erase(cur_waiter);
            }
        }
    }
}

void ConcurrencyControlFairRoundRobinScheduler::notifyAcquired()
{
    if (state.total_demanders.load(std::memory_order_relaxed) == 0)
        return;
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ true);
}

void ConcurrencyControlFairRoundRobinScheduler::notifyDemand()
{
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// See ConcurrencyControlRoundRobinScheduler::reclaim for the contract: decrement `allocated`
// (not `released`), re-add to waiters if the allocation was previously saturated.
void ConcurrencyControlFairRoundRobinScheduler::reclaim(Allocation * allocation, SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    removeDemanderLocked(allocation);

    {
        std::unique_lock alock{allocation->mutex};
        bool was_waiter = allocation->allocated < allocation->limit;
        allocation->allocated -= amount;

        if (!was_waiter && allocation->allocated < allocation->limit)
            allocation->waiter = waiters.insert(cur_waiter, allocation);
    }

    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// See ConcurrencyControlRoundRobinScheduler::Allocation::setMoreDemand for contract.
void ConcurrencyControlFairRoundRobinScheduler::Allocation::setMoreDemand(bool value)
{
    bool prev = has_more_demand.exchange(value, std::memory_order_acq_rel);
    if (!value && prev)
    {
        SlotCount to_reclaim = granted.exchange(0, std::memory_order_acq_rel);
        if (to_reclaim > 0)
        {
            parent.reclaim(this, to_reclaim);
        }
        else
        {
            std::unique_lock lock{parent.state.mutex};
            parent.removeDemanderLocked(this);
        }
    }
    else if (value && !prev)
    {
        // Only add to demanders if still a waiter (preserves demanders ⊆ waiters invariant).
        std::unique_lock lock{parent.state.mutex};
        if (allocated < limit)
            parent.addDemanderLocked(this);
        parent.parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
    }
}


bool ConcurrencyControlMaxMinFairScheduler::AllocationCompare::operator()(const Allocation & lhs, const Allocation & rhs) const
{
    // Primary: sort by allocated count (minimum first for max-min fairness)
    // Secondary: sort by sequence_number for FIFO ordering when allocated counts are equal
    if (lhs.allocated != rhs.allocated)
        return lhs.allocated < rhs.allocated;
    return lhs.sequence_number < rhs.sequence_number;
}

ConcurrencyControlMaxMinFairScheduler::Slot::Slot(SlotAllocationPtr && allocation_, bool competing_, size_t slot_id_)
    : IAcquiredSlot(slot_id_)
    , allocation(std::move(allocation_))
    , competing(competing_)
    , acquired_slot_increment(competing ? CurrentMetrics::ConcurrencyControlAcquired : CurrentMetrics::ConcurrencyControlAcquiredNonCompeting)
{
}

ConcurrencyControlMaxMinFairScheduler::Slot::~Slot()
{
    if (competing)
        static_cast<ConcurrencyControlMaxMinFairScheduler::Allocation&>(*allocation).release();
}

ConcurrencyControlMaxMinFairScheduler::Allocation::Allocation(ConcurrencyControlMaxMinFairScheduler & parent_, SlotCount min_, SlotCount max, SlotCount granted_, UInt64 sequence_number_)
    : parent(parent_)
    , min(min_)
    , limit(max - min)
    , allocated(granted_)
    , noncompeting(min)
    , granted(granted_)
    , sequence_number(sequence_number_)
{
}

ConcurrencyControlMaxMinFairScheduler::Allocation::~Allocation()
{
    // We have to lock parent's mutex to avoid race with grant()
    // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
    parent.free(this);
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlMaxMinFairScheduler::Allocation::tryAcquire()
{
    // First try acquire non-competing slot (if any)
    SlotCount value = noncompeting.load();
    while (value)
    {
        if (noncompeting.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquiredNonCompeting, 1);
            AcquiredSlotPtr result;
            {
                std::unique_lock lock{mutex};
                result = AcquiredSlotPtr(new Slot(shared_from_this(), false, last_slot_id++)); // can't use std::make_shared due to private ctor
            }
            // Targeted grant: give this allocation 1 more competing slot if available
            parent.notifyAcquired();
            return result;
        }
    }

    // If all non-competing slots are already acquired - try acquire granted (competing) slot
    value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            AcquiredSlotPtr result;
            {
                std::unique_lock lock{mutex};
                result = AcquiredSlotPtr(new Slot(shared_from_this(), true, last_slot_id++)); // can't use std::make_shared due to private ctor
            }
            parent.notifyAcquired();
            return result;
        }
    }

    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlMaxMinFairScheduler::Allocation::acquire()
{
    auto result = tryAcquire();
    chassert(result);
    return result;
}

// Grant single slot to allocation returns true iff more slot(s) are required
bool ConcurrencyControlMaxMinFairScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

// Release one slot and grant it to other allocation if required
void ConcurrencyControlMaxMinFairScheduler::Allocation::release()
{
    parent.release(1);
    std::unique_lock lock{mutex};
    released++;
    if (released > allocated)
        abort();
}

ConcurrencyControlMaxMinFairScheduler::ConcurrencyControlMaxMinFairScheduler(ConcurrencyControl & parent_, ConcurrencyControlState & state_)
    : parent(parent_)
    , state(state_)
{
}

ConcurrencyControlMaxMinFairScheduler::~ConcurrencyControlMaxMinFairScheduler()
{
    if (!waiters.empty())
        abort();
}

void ConcurrencyControlMaxMinFairScheduler::addDemanderLocked(Allocation * allocation)
{
    if (!allocation->demanders_hook.is_linked())
    {
        demanders.insert(*allocation);
        state.total_demanders.fetch_add(1, std::memory_order_relaxed);
    }
}

void ConcurrencyControlMaxMinFairScheduler::removeDemanderLocked(Allocation * allocation)
{
    if (allocation->demanders_hook.is_linked())
    {
        demanders.erase(demanders.iterator_to(*allocation));
        state.total_demanders.fetch_sub(1, std::memory_order_relaxed);
    }
}

SlotAllocationPtr ConcurrencyControlMaxMinFairScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Do not count `min` slots towards the limit. They are NOT considered as taking part in competition.
    // Lazy allocation: grant at most 1 competing slot from available capacity for ramp-up bootstrap.
    // Eager path (state.lazy_allocation=false) restores the pre-#88339 behavior — used as an
    // emergency rollback lever only.
    SlotCount limit = max - min;
    SlotCount granted = state.lazy_allocation.load(std::memory_order_relaxed)
        ? std::min(std::min(SlotCount(1), limit), state.available(lock))
        : std::min(limit, state.available(lock));
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation with monotonically increasing sequence number for FIFO ordering
    auto allocation = SlotAllocationPtr(new Allocation(*this, min, max, granted, next_sequence_number++));

    // Start waiting if more slots are required
    if (granted < limit)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, limit - granted);
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        // Insert into waiters set (sorted by allocated count, then by sequence number)
        // The hook's is_linked() will return true after insertion
        auto * a = static_cast<Allocation*>(allocation.get());
        waiters.insert(*a);
        // New allocation defaults to has_more_demand == true; register it in demanders.
        addDemanderLocked(a);
    }

    return allocation;
}

void ConcurrencyControlMaxMinFairScheduler::free(Allocation * allocation)
{
    // Allocation is allowed to be canceled even if there are:
    //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
    //  - `waiter`: active waiting for more slots to be allocated
    // Thus Allocation destruction may require the following lock, to avoid race conditions
    std::unique_lock lock{state.mutex};
    auto [amount, is_waiting] = allocation->cancel();

    state.cur_concurrency -= amount;
    removeDemanderLocked(allocation);
    if (is_waiting)
        waiters.erase(waiters.iterator_to(*allocation));
    // Lazy multi: skip waiters with pending grants (min-level only in MMF) to avoid
    // piling additional grants onto allocations that haven't consumed their existing ones.
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlMaxMinFairScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// Max-min fair scheduling — tri-mode.
// Bulk mode: original eager distribution (setMaxConcurrency — all capacity must be distributed).
// Lazy mode: iterate the `demanders` set (subset of waiters with has_more_demand==true).
//   At the minimum allocation level among demanders, find one with granted==0 and grant.
//
//   IMPORTANT: lazy schedule MUST NOT descend past min_level. Walking to higher-allocated
//   demanders would violate max-min fairness: an allocation that already has more slots than
//   its peers would receive yet another slot while a lower-allocated peer at min_level is
//   temporarily pending. The min_level stop keeps the scheduler fair on every single grant.
//   If every min_level demander has a pending grant, we deliberately stall this round and let
//   the next notifyAcquired call (which happens when one of them actually consumes its slot)
//   reschedule. Do NOT "simplify" this by removing the min_level guard.
//
//   Iterating demanders (not all waiters) makes the hot path scale with actively demanding
//   queries, not the full waiter list.
void ConcurrencyControlMaxMinFairScheduler::schedule(std::unique_lock<std::mutex> &, bool lazy_grant, bool single_grant)
{
    if (lazy_grant)
    {
        while (state.cur_concurrency < state.max_concurrency && !demanders.empty())
        {
            // Demanders are sorted by (allocated, sequence_number) just like waiters.
            // The first element is the minimum-allocated demander.
            auto it = demanders.begin();
            SlotCount min_level = it->allocated;

            // At min_level, find a demander with no pending grant.
            // Do NOT descend past min_level — preserves max-min fairness among actual demanders.
            bool granted_one = false;
            while (it != demanders.end() && it->allocated == min_level)
            {
                if (it->granted.load(std::memory_order_relaxed) == 0)
                {
                    state.cur_concurrency++;
                    Allocation & allocation = *it;
                    // Pre-emptively remove from both sets; `grant()` mutates `allocated`, which
                    // is the sort key for both intrusive sets — mutating it while linked would
                    // corrupt container ordering. Re-insert after grant() if still a waiter.
                    // Route through removeDemanderLocked so `total_demanders` stays consistent.
                    removeDemanderLocked(&allocation);
                    waiters.erase(waiters.iterator_to(allocation));
                    chassert(!allocation.waiters_hook.is_linked());
                    chassert(!allocation.demanders_hook.is_linked());
                    SlotCount allocated_before = allocation.allocated;
                    bool still_waiter = allocation.grant();
                    chassert(allocation.allocated == allocated_before + 1);
                    if (still_waiter)
                    {
                        waiters.insert(allocation);
                        // Re-insert into demanders only if has_more_demand is still true.
                        // A concurrent setMoreDemand(false) could have flipped it; the atomic
                        // exchange there will remove from demanders via removeDemanderLocked.
                        // We check the flag here for consistency.
                        if (allocation.has_more_demand.load(std::memory_order_relaxed))
                            addDemanderLocked(&allocation);
                    }
                    granted_one = true;
                    break;
                }
                ++it;
            }
            if (!granted_one)
                return; // All min-level demanders have pending grants; wait for notifyAcquired.
            if (single_grant)
                return;
            // Loop: next iteration re-evaluates min_level (may have changed after re-insert)
        }
    }
    else
    {
        // Bulk mode: original eager distribution over all waiters.
        // Must erase from both waiters AND demanders before grant() mutates `allocated`
        // (the sort key for both intrusive sets) — leaving the allocation linked while
        // its key changes would corrupt container ordering.
        //
        // cur_concurrency++ happens before grant(). This is safe because we hold state.mutex
        // across the whole iteration and grant() is infallible (it only increments counters
        // under allocation.mutex; cannot throw, cannot abort). If future edits make grant()
        // fallible, move the increment after grant() succeeds.
        while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
        {
            state.cur_concurrency++;

            auto it = waiters.begin();
            Allocation & allocation = *it;

            bool was_demander = allocation.demanders_hook.is_linked();
            if (was_demander)
                removeDemanderLocked(&allocation);
            waiters.erase(it);
            chassert(!allocation.waiters_hook.is_linked());
            chassert(!allocation.demanders_hook.is_linked());

            SlotCount allocated_before = allocation.allocated;
            bool still_waiter = allocation.grant();
            chassert(allocation.allocated == allocated_before + 1);
            if (still_waiter)
            {
                waiters.insert(allocation);
                // Re-insert into demanders iff it was there AND demand is still set.
                // A concurrent setMoreDemand(false) can't race here because we hold state.mutex,
                // but the atomic has_more_demand could theoretically be stale — in that case the
                // scheduler may briefly consider this allocation, which is harmless (next lazy
                // schedule iteration will observe the updated flag via removeDemanderLocked).
                if (was_demander && allocation.has_more_demand.load(std::memory_order_relaxed))
                    addDemanderLocked(&allocation);
            }
            // If !still_waiter: allocation is no longer a waiter, and we already removed it
            // from demanders above. Nothing to do.
        }
    }
}

void ConcurrencyControlMaxMinFairScheduler::notifyAcquired()
{
    if (state.total_demanders.load(std::memory_order_relaxed) == 0)
        return;
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ true);
}

void ConcurrencyControlMaxMinFairScheduler::notifyDemand()
{
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlMaxMinFairScheduler::reclaim(Allocation * allocation, SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;

    // MMF orders waiters by `allocated`. For fairness, reclaimed slots must not continue
    // counting against the allocation — decrement `allocated` and re-position the waiter
    // so subsequent schedule() calls give this allocation its fair share.
    // Also remove from demanders (reclaim is triggered by setMoreDemand(false)).
    removeDemanderLocked(allocation);

    bool was_in_set = allocation->waiters_hook.is_linked();
    if (was_in_set)
        waiters.erase(waiters.iterator_to(*allocation));

    {
        std::unique_lock alock{allocation->mutex};
        allocation->allocated -= amount;
    }

    // Re-insert with the updated `allocated` (places it correctly in the sort order).
    // Also covers the case where the allocation was previously saturated (allocated == limit,
    // not in set) and now has capacity to receive more.
    if (allocation->allocated < allocation->limit)
        waiters.insert(*allocation);

    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// See ConcurrencyControlRoundRobinScheduler::Allocation::setMoreDemand for contract.
void ConcurrencyControlMaxMinFairScheduler::Allocation::setMoreDemand(bool value)
{
    bool prev = has_more_demand.exchange(value, std::memory_order_acq_rel);
    if (!value && prev)
    {
        SlotCount to_reclaim = granted.exchange(0, std::memory_order_acq_rel);
        if (to_reclaim > 0)
        {
            parent.reclaim(this, to_reclaim);
        }
        else
        {
            std::unique_lock lock{parent.state.mutex};
            parent.removeDemanderLocked(this);
        }
    }
    else if (value && !prev)
    {
        // Only add to demanders if still a waiter — for MMF, waiters_hook.is_linked() is the
        // authoritative check. This preserves the demanders ⊆ waiters invariant.
        std::unique_lock lock{parent.state.mutex};
        if (waiters_hook.is_linked())
            parent.addDemanderLocked(this);
        parent.parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
    }
}


ConcurrencyControl::ConcurrencyControl()
    : round_robin(*this, state)
    , fair_round_robin(*this, state)
    , max_min_fair(*this, state)
{
}

ConcurrencyControl & ConcurrencyControl::instance()
{
    static ConcurrencyControl result;
    return result;
}

[[nodiscard]] SlotAllocationPtr ConcurrencyControl::allocate(SlotCount min, SlotCount max)
{
    if (min > max)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ConcurrencyControl: invalid allocation requirements");

    std::unique_lock lock{state.mutex};
    switch (scheduler)
    {
        case Scheduler::RoundRobin:
            return round_robin.allocate(lock, min, max);
        case Scheduler::FairRoundRobin:
            return fair_round_robin.allocate(lock, min, max);
        case Scheduler::MaxMinFair:
            return max_min_fair.allocate(lock, min, max);
    }
}

void ConcurrencyControl::setMaxConcurrency(SlotCount value)
{
    std::unique_lock lock{state.mutex};
    state.max_concurrency = std::max<SlotCount>(1, value); // never allow max_concurrency to be zero
    state.max_concurrency_metric.changeTo(state.max_concurrency == UnlimitedSlots ? 0 : state.max_concurrency);
    schedule(lock);
}

bool ConcurrencyControl::setScheduler(const String & value)
{
    std::unique_lock lock{state.mutex};
    if (value == "fair_round_robin")
    {
        scheduler = Scheduler::FairRoundRobin;
        return true;
    }
    if (value == "round_robin")
    {
        scheduler = Scheduler::RoundRobin;
        return true;
    }
    if (value == "max_min_fair")
    {
        scheduler = Scheduler::MaxMinFair;
        return true;
    }
    return false; // invalid value - stick to the current scheduler
}

String ConcurrencyControl::getScheduler() const
{
    std::unique_lock lock{state.mutex};
    switch (scheduler)
    {
        case Scheduler::RoundRobin: return "round_robin";
        case Scheduler::FairRoundRobin: return "fair_round_robin";
        case Scheduler::MaxMinFair: return "max_min_fair";
    }
}

void ConcurrencyControl::setLazyAllocation(bool value)
{
    // Atomic — no lock needed. Only affects allocations made after this point.
    state.lazy_allocation.store(value, std::memory_order_relaxed);
}

bool ConcurrencyControl::getLazyAllocation() const
{
    return state.lazy_allocation.load(std::memory_order_relaxed);
}

void ConcurrencyControl::schedule(std::unique_lock<std::mutex> & lock, bool lazy_grant, bool single_grant)
{
    switch (scheduler)
    {
        case Scheduler::RoundRobin:
            fair_round_robin.schedule(lock, lazy_grant, single_grant); // first schedule from old scheduler (works only during transition period)
            max_min_fair.schedule(lock, lazy_grant, single_grant);
            round_robin.schedule(lock, lazy_grant, single_grant);
            return;
        case Scheduler::FairRoundRobin:
            round_robin.schedule(lock, lazy_grant, single_grant); // first schedule from old scheduler (works only during transition period)
            max_min_fair.schedule(lock, lazy_grant, single_grant);
            fair_round_robin.schedule(lock, lazy_grant, single_grant);
            return;
        case Scheduler::MaxMinFair:
            round_robin.schedule(lock, lazy_grant, single_grant); // first schedule from old scheduler (works only during transition period)
            fair_round_robin.schedule(lock, lazy_grant, single_grant);
            max_min_fair.schedule(lock, lazy_grant, single_grant);
            return;
    }
}

}
