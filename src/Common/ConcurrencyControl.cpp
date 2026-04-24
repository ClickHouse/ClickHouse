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
{
}

ConcurrencyControlRoundRobinScheduler::~ConcurrencyControlRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

void ConcurrencyControlRoundRobinScheduler::addWaiterLocked(Allocation * allocation)
{
    allocation->waiter = waiters.insert(cur_waiter, allocation);
    state.total_waiters.fetch_add(1, std::memory_order_relaxed);
}

void ConcurrencyControlRoundRobinScheduler::removeWaiterLocked(Allocation * allocation)
{
    if (cur_waiter == allocation->waiter)
        cur_waiter = waiters.erase(allocation->waiter);
    else
        waiters.erase(allocation->waiter);
    state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
}

SlotAllocationPtr ConcurrencyControlRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Lazy allocation: grant `min` slots unconditionally (oversubscription allowed),
    // plus at most 1 additional slot from available capacity for ramp-up bootstrap.
    // Remaining slots are granted one-at-a-time via lazy schedule() as they are actually acquired.
    //
    // `eager_granted` = what an eager allocate would have handed out given current capacity.
    // We use it only to attribute "Delayed" correctly: only slots that couldn't be granted
    // due to capacity pressure count as delayed, not ones intentionally withheld.
    SlotCount eager_granted = std::max(min, std::min(max, state.available(lock)));
    SlotCount capacity_delay = max - eager_granted;

    SlotCount granted = min;
    if (granted < max && state.available(lock) > min)
        granted += 1;
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation and start waiting if more slots are required
    if (granted < max)
    {
        if (capacity_delay > 0)
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, capacity_delay);
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        }
        auto alloc = SlotAllocationPtr(new Allocation(*this, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
        state.total_waiters.fetch_add(1, std::memory_order_relaxed);
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
    if (waiter)
        removeWaiterLocked(allocation);
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
// Lazy multi (lazy_grant=true, single_grant=false): skip waiters with pending grants,
//   grant multiple (used by release/free/setMax to redistribute freed capacity).
// Lazy single (lazy_grant=true, single_grant=true): skip pending, grant at most 1
//   (used by notifyAcquired for one-at-a-time ramp-up).
void ConcurrencyControlRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &, bool lazy_grant, bool single_grant)
{
    if (lazy_grant)
    {
        // Skip waiters with pending grants (granted > 0) to avoid piling grants onto
        // allocations that haven't consumed their existing ones. This paces the grant
        // rate to actual slot consumption, so an idle allocation never holds more than
        // one unused pending slot.
        size_t skipped = 0;
        while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
        {
            if (cur_waiter == waiters.end())
                cur_waiter = waiters.begin();
            Allocation * allocation = *cur_waiter;

            if (allocation->granted.load(std::memory_order_relaxed) > 0)
            {
                ++cur_waiter;
                if (++skipped > waiters.size())
                    break; // Every waiter has a pending grant
                continue;
            }

            skipped = 0;
            state.cur_concurrency++;
            bool still_waiter = allocation->grant();
            ++cur_waiter;
            if (!still_waiter)
            {
                // Allocation reached its limit — remove from waiters list.
                if (cur_waiter == allocation->waiter)
                    cur_waiter = waiters.erase(allocation->waiter);
                else
                    waiters.erase(allocation->waiter);
                state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
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
                cur_waiter = waiters.erase(cur_waiter);
                state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }
}

void ConcurrencyControlRoundRobinScheduler::notifyAcquired()
{
    // Fast path: if no waiter anywhere, no one to grant to — skip the state.mutex acquisition.
    if (state.total_waiters.load(std::memory_order_relaxed) == 0)
        return;
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ true);
}

// Raise or lower the allocation's max slot ceiling.
// Grow past saturation → re-insert into waiters and trigger a schedule round.
// Shrink past saturation → remove from waiters so the scheduler stops granting more.
// Already-granted slots (including pending-but-not-acquired) are NOT reclaimed; they
// count against cur_concurrency until the allocation is freed.
//
// MUST NOT be called while holding state.mutex — we take it here internally.
void ConcurrencyControlRoundRobinScheduler::Allocation::setMax(SlotCount new_max)
{
    std::unique_lock lock{parent.state.mutex};
    bool need_schedule = false;
    {
        std::unique_lock alock{mutex};
        if (new_max == limit)
            return;
        bool was_waiter = (allocated < limit);
        bool will_be_waiter = (allocated < new_max);
        SlotCount old_limit = limit;
        limit = new_max;
        if (!was_waiter && will_be_waiter)
        {
            // Grew past saturation — re-insert.
            waiter = parent.waiters.insert(parent.cur_waiter, this);
            parent.state.total_waiters.fetch_add(1, std::memory_order_relaxed);
            need_schedule = true;
        }
        else if (was_waiter && !will_be_waiter)
        {
            // Shrunk past saturation — remove from waiters so free() sees consistent state.
            if (parent.cur_waiter == waiter)
                parent.cur_waiter = parent.waiters.erase(waiter);
            else
                parent.waiters.erase(waiter);
            parent.state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
        }
        else if (was_waiter && will_be_waiter && new_max > old_limit)
        {
            // Still a waiter but grew — trigger schedule so free capacity can flow to this
            // allocation immediately (not wait for an unrelated release/notifyAcquired).
            need_schedule = true;
        }
    }
    if (need_schedule)
        parent.parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
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

bool ConcurrencyControlFairRoundRobinScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

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
{
}

ConcurrencyControlFairRoundRobinScheduler::~ConcurrencyControlFairRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

void ConcurrencyControlFairRoundRobinScheduler::addWaiterLocked(Allocation * allocation)
{
    allocation->waiter = waiters.insert(cur_waiter, allocation);
    state.total_waiters.fetch_add(1, std::memory_order_relaxed);
}

void ConcurrencyControlFairRoundRobinScheduler::removeWaiterLocked(Allocation * allocation)
{
    if (cur_waiter == allocation->waiter)
        cur_waiter = waiters.erase(allocation->waiter);
    else
        waiters.erase(allocation->waiter);
    state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
}

SlotAllocationPtr ConcurrencyControlFairRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Do not count `min` slots towards the limit. They are NOT considered as taking part in competition.
    // Lazy allocation: grant at most 1 competing slot from available capacity for ramp-up bootstrap.
    SlotCount limit = max - min;
    SlotCount eager_granted = std::min(limit, state.available(lock));
    SlotCount capacity_delay = limit - eager_granted;

    SlotCount granted = std::min({SlotCount(1), limit, state.available(lock)});
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    if (granted < limit)
    {
        if (capacity_delay > 0)
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, capacity_delay);
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        }
        auto alloc = SlotAllocationPtr(new Allocation(*this, min, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
        state.total_waiters.fetch_add(1, std::memory_order_relaxed);
        return alloc;
    }
    else
    {
        return SlotAllocationPtr(new Allocation(*this, min, max, granted));
    }
}

void ConcurrencyControlFairRoundRobinScheduler::free(Allocation * allocation)
{
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (waiter)
        removeWaiterLocked(allocation);
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlFairRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlFairRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &, bool lazy_grant, bool single_grant)
{
    if (lazy_grant)
    {
        size_t skipped = 0;
        while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
        {
            if (cur_waiter == waiters.end())
                cur_waiter = waiters.begin();
            Allocation * allocation = *cur_waiter;

            if (allocation->granted.load(std::memory_order_relaxed) > 0)
            {
                ++cur_waiter;
                if (++skipped > waiters.size())
                    break;
                continue;
            }

            skipped = 0;
            state.cur_concurrency++;
            bool still_waiter = allocation->grant();
            ++cur_waiter;
            if (!still_waiter)
            {
                if (cur_waiter == allocation->waiter)
                    cur_waiter = waiters.erase(allocation->waiter);
                else
                    waiters.erase(allocation->waiter);
                state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
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
                cur_waiter = waiters.erase(cur_waiter);
                state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }
}

void ConcurrencyControlFairRoundRobinScheduler::notifyAcquired()
{
    if (state.total_waiters.load(std::memory_order_relaxed) == 0)
        return;
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ true);
}

// See ConcurrencyControlRoundRobinScheduler::Allocation::setMax for contract.
void ConcurrencyControlFairRoundRobinScheduler::Allocation::setMax(SlotCount new_max)
{
    // new_max in ISlotAllocation space (total = min + competing). FRR tracks only competing
    // slots in `limit`, so translate.
    SlotCount new_competing_limit = (new_max > min) ? (new_max - min) : 0;

    std::unique_lock lock{parent.state.mutex};
    bool need_schedule = false;
    {
        std::unique_lock alock{mutex};
        if (new_competing_limit == limit)
            return;
        bool was_waiter = (allocated < limit);
        bool will_be_waiter = (allocated < new_competing_limit);
        SlotCount old_limit = limit;
        limit = new_competing_limit;
        if (!was_waiter && will_be_waiter)
        {
            waiter = parent.waiters.insert(parent.cur_waiter, this);
            parent.state.total_waiters.fetch_add(1, std::memory_order_relaxed);
            need_schedule = true;
        }
        else if (was_waiter && !will_be_waiter)
        {
            if (parent.cur_waiter == waiter)
                parent.cur_waiter = parent.waiters.erase(waiter);
            else
                parent.waiters.erase(waiter);
            parent.state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
        }
        else if (was_waiter && will_be_waiter && new_competing_limit > old_limit)
        {
            // See RR::setMax.
            need_schedule = true;
        }
    }
    if (need_schedule)
        parent.parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
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
    parent.free(this);
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlMaxMinFairScheduler::Allocation::tryAcquire()
{
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
            parent.notifyAcquired();
            return result;
        }
    }

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

    return {};
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControlMaxMinFairScheduler::Allocation::acquire()
{
    auto result = tryAcquire();
    chassert(result);
    return result;
}

bool ConcurrencyControlMaxMinFairScheduler::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

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

void ConcurrencyControlMaxMinFairScheduler::addWaiterLocked(Allocation * allocation)
{
    waiters.insert(*allocation);
    state.total_waiters.fetch_add(1, std::memory_order_relaxed);
}

void ConcurrencyControlMaxMinFairScheduler::removeWaiterLocked(Allocation * allocation)
{
    waiters.erase(waiters.iterator_to(*allocation));
    state.total_waiters.fetch_sub(1, std::memory_order_relaxed);
}

SlotAllocationPtr ConcurrencyControlMaxMinFairScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    SlotCount limit = max - min;
    SlotCount eager_granted = std::min(limit, state.available(lock));
    SlotCount capacity_delay = limit - eager_granted;

    SlotCount granted = std::min({SlotCount(1), limit, state.available(lock)});
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    auto allocation = SlotAllocationPtr(new Allocation(*this, min, max, granted, next_sequence_number++));

    if (granted < limit)
    {
        if (capacity_delay > 0)
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, capacity_delay);
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        }
        auto * a = static_cast<Allocation*>(allocation.get());
        addWaiterLocked(a);
    }

    return allocation;
}

void ConcurrencyControlMaxMinFairScheduler::free(Allocation * allocation)
{
    std::unique_lock lock{state.mutex};
    auto [amount, is_waiting] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (is_waiting)
        removeWaiterLocked(allocation);
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

void ConcurrencyControlMaxMinFairScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
}

// Max-min fair scheduling.
// Bulk mode: original eager distribution (setMaxConcurrency — all capacity must be distributed).
// Lazy mode: work-conserving. Waiters are sorted ascending by (allocated, sequence_number), so
//   iterating from begin() and taking the first with granted==0 picks the most-deserving
//   eligible waiter. If a lower-allocated waiter has a pending grant, we don't stall — we
//   advance to the next eligible waiter. Long-term max-min fairness still holds because
//   the sort order always favors the lowest-allocated eligible waiter when it's ready.
void ConcurrencyControlMaxMinFairScheduler::schedule(std::unique_lock<std::mutex> &, bool lazy_grant, bool single_grant)
{
    if (lazy_grant)
    {
        while (state.cur_concurrency < state.max_concurrency && !waiters.empty())
        {
            bool granted_one = false;
            for (auto & waiter : waiters)
            {
                if (waiter.granted.load(std::memory_order_relaxed) == 0)
                {
                    state.cur_concurrency++;
                    Allocation & allocation = waiter;
                    // Pre-emptively remove; `grant()` mutates `allocated` (the sort key) so
                    // leaving it linked would corrupt the set's ordering. Re-insert after.
                    removeWaiterLocked(&allocation);
                    chassert(!allocation.waiters_hook.is_linked());
                    SlotCount allocated_before = allocation.allocated;
                    bool still_waiter = allocation.grant();
                    chassert(allocation.allocated == allocated_before + 1);
                    if (still_waiter)
                        addWaiterLocked(&allocation);
                    granted_one = true;
                    break;
                }
            }
            if (!granted_one)
                return; // Every waiter has a pending grant; wait for notifyAcquired.
            if (single_grant)
                return;
        }
    }
    else
    {
        // Bulk mode: original eager distribution over all waiters.
        // cur_concurrency++ happens before grant(). This is safe because we hold state.mutex
        // and grant() is infallible (only increments counters under allocation.mutex).
        while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
        {
            state.cur_concurrency++;

            auto it = waiters.begin();
            Allocation & allocation = *it;

            removeWaiterLocked(&allocation);
            chassert(!allocation.waiters_hook.is_linked());

            SlotCount allocated_before = allocation.allocated;
            bool still_waiter = allocation.grant();
            chassert(allocation.allocated == allocated_before + 1);
            if (still_waiter)
                addWaiterLocked(&allocation);
        }
    }
}

void ConcurrencyControlMaxMinFairScheduler::notifyAcquired()
{
    if (state.total_waiters.load(std::memory_order_relaxed) == 0)
        return;
    std::unique_lock lock{state.mutex};
    parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ true);
}

// See ConcurrencyControlRoundRobinScheduler::Allocation::setMax for contract.
// MMF sorts waiters by `allocated`; since setMax only modifies `limit` (not allocated),
// the sort position is unchanged — we don't need the erase/insert dance that grant() needs.
void ConcurrencyControlMaxMinFairScheduler::Allocation::setMax(SlotCount new_max)
{
    SlotCount new_competing_limit = (new_max > min) ? (new_max - min) : 0;

    std::unique_lock lock{parent.state.mutex};
    bool need_schedule = false;
    {
        std::unique_lock alock{mutex};
        if (new_competing_limit == limit)
            return;
        bool was_waiter = (allocated < limit);
        bool will_be_waiter = (allocated < new_competing_limit);
        SlotCount old_limit = limit;
        limit = new_competing_limit;
        if (!was_waiter && will_be_waiter)
        {
            parent.addWaiterLocked(this);
            need_schedule = true;
        }
        else if (was_waiter && !will_be_waiter)
        {
            parent.removeWaiterLocked(this);
        }
        else if (was_waiter && will_be_waiter && new_competing_limit > old_limit)
        {
            // See RR::setMax.
            need_schedule = true;
        }
    }
    if (need_schedule)
        parent.parent.schedule(lock, /*lazy_grant=*/ true, /*single_grant=*/ false);
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
            round_robin.schedule(lock, lazy_grant, single_grant);
            max_min_fair.schedule(lock, lazy_grant, single_grant);
            fair_round_robin.schedule(lock, lazy_grant, single_grant);
            return;
        case Scheduler::MaxMinFair:
            round_robin.schedule(lock, lazy_grant, single_grant);
            fair_round_robin.schedule(lock, lazy_grant, single_grant);
            max_min_fair.schedule(lock, lazy_grant, single_grant);
            return;
    }
}

}
