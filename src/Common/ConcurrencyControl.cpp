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


namespace
{
    /// Record an attempted ceiling raise that available capacity couldn't fully satisfy.
    /// Bumps `ConcurrencyControlSlotsDelayed` by the shortfall and, the first time it
    /// observes pressure on a given allocation, bumps `ConcurrencyControlQueriesDelayed`
    /// exactly once. `query_counted` lives on the allocation so the once-per-allocation
    /// guarantee survives multiple calls from `allocate` and subsequent `setMax` grows.
    void emitDelayedOnGrow(bool & query_counted, SlotCount wanted, SlotCount available)
    {
        if (wanted <= available)
            return;
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, wanted - available);
        if (!query_counted)
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
            query_counted = true;
        }
    }
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
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this(), last_slot_id++)); // can't use std::make_shared due to private ctor
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
}

void ConcurrencyControlRoundRobinScheduler::removeWaiterLocked(Allocation * allocation)
{
    if (cur_waiter == allocation->waiter)
        cur_waiter = waiters.erase(allocation->waiter);
    else
        waiters.erase(allocation->waiter);
}

SlotAllocationPtr ConcurrencyControlRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Grant exactly `min` slots up front. Oversubscription is allowed (RR doesn't gate min
    // by capacity). Then run `schedule` so that any remaining capacity flows into this
    // allocation up to its `max` -- this is what gives callers the eager `allocate(min, max)`
    // semantics they expect when they don't intend to grow via `setMax`.
    const SlotCount granted = min;
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    SlotAllocationPtr alloc;
    if (granted < max)
    {
        alloc = SlotAllocationPtr(new Allocation(*this, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
    }
    else
    {
        alloc = SlotAllocationPtr(new Allocation(*this, max, granted));
    }
    // Emit delayed metrics for any portion of `max - min` that current capacity can't
    // cover. This catches the eager rollback path (`allocate(1, num_threads)`) which
    // never calls `setMax` and would otherwise leave the metric at zero under pressure.
    if (max > min)
    {
        const SlotCount delta = max - min;
        const SlotCount available = state.available(lock);
        emitDelayedOnGrow(static_cast<Allocation *>(alloc.get())->query_counted, delta, available);
    }
    // Fill the new allocation up to `max` from available capacity (also benefits any other
    // waiters that have free room). For lazy callers (allocate(1, 1)), this is a no-op.
    parent.schedule(lock);
    return alloc;
}

void ConcurrencyControlRoundRobinScheduler::free(Allocation * allocation)
{
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (waiter)
        removeWaiterLocked(allocation);
    parent.schedule(lock);
}

void ConcurrencyControlRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock);
}

// Round-robin scheduling of available slots among waiting allocations.
// Single mode: walk the waiter list, grant up to each waiter's current `limit` while
// capacity allows. The limit IS the demand signal (set by the caller via setMax).
void ConcurrencyControlRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &)
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
        }
    }
}

// Raise or lower the allocation's max slot ceiling. Emits SlotsDelayed/QueriesDelayed when
// the grow request exceeds current capacity. Grow past saturation re-inserts into waiters;
// shrink past saturation removes from waiters. Already-granted slots are not reclaimed.
//
// MUST NOT be called while holding state.mutex -- we take it here internally.
void ConcurrencyControlRoundRobinScheduler::Allocation::setMax(SlotCount new_max)
{
    chassert(new_max > 0);
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
            // Grew past saturation -- re-insert.
            waiter = parent.waiters.insert(parent.cur_waiter, this);
            need_schedule = true;
        }
        else if (was_waiter && !will_be_waiter)
        {
            // Shrunk past saturation -- remove from waiters so `free` sees consistent state.
            if (parent.cur_waiter == waiter)
                parent.cur_waiter = parent.waiters.erase(waiter);
            else
                parent.waiters.erase(waiter);
        }
        else if (was_waiter && will_be_waiter && new_max > old_limit)
        {
            // Still a waiter but grew.
            need_schedule = true;
        }

        if (new_max > old_limit)
        {
            // Only count slots beyond what was previously asked-for AND beyond what's
            // already allocated. `allocated > old_limit` is possible after a prior
            // shrink (setMax does not reclaim slots); without the std::max, the
            // subtraction would underflow on this path.
            const SlotCount baseline = std::max(old_limit, allocated);
            if (new_max > baseline)
            {
                const SlotCount delta = new_max - baseline;
                const SlotCount available = parent.state.available(lock);
                emitDelayedOnGrow(query_counted, delta, available);
            }
        }
    }
    if (need_schedule)
        parent.parent.schedule(lock);
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
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this(), false, last_slot_id++)); // can't use std::make_shared due to private ctor
        }
    }

    // If all non-competing slots are already acquired - try acquire granted (competing) slot
    value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this(), true, last_slot_id++)); // can't use std::make_shared due to private ctor
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
}

void ConcurrencyControlFairRoundRobinScheduler::removeWaiterLocked(Allocation * allocation)
{
    if (cur_waiter == allocation->waiter)
        cur_waiter = waiters.erase(allocation->waiter);
    else
        waiters.erase(allocation->waiter);
}

SlotAllocationPtr ConcurrencyControlFairRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Min slots are non-competing -- they don't count toward the global limit. Grant zero
    // competing slots up front; `schedule` below then fills up to the competing limit.
    SlotCount limit = max - min;
    SlotCount granted = 0;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    SlotAllocationPtr alloc;
    if (granted < limit)
    {
        alloc = SlotAllocationPtr(new Allocation(*this, min, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
    }
    else
    {
        alloc = SlotAllocationPtr(new Allocation(*this, min, max, granted));
    }
    // See RR allocate for the rationale. FRR's competing demand is `max - min` (= limit).
    if (limit > 0)
    {
        const SlotCount available = state.available(lock);
        emitDelayedOnGrow(static_cast<Allocation *>(alloc.get())->query_counted, limit, available);
    }
    parent.schedule(lock);
    return alloc;
}

void ConcurrencyControlFairRoundRobinScheduler::free(Allocation * allocation)
{
    std::unique_lock lock{state.mutex};
    auto [amount, waiter] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (waiter)
        removeWaiterLocked(allocation);
    parent.schedule(lock);
}

void ConcurrencyControlFairRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock);
}

void ConcurrencyControlFairRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &)
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
        }
    }
}

// See ConcurrencyControlRoundRobinScheduler::Allocation::setMax for contract.
void ConcurrencyControlFairRoundRobinScheduler::Allocation::setMax(SlotCount new_max)
{
    chassert(new_max > 0);
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
            need_schedule = true;
        }
        else if (was_waiter && !will_be_waiter)
        {
            if (parent.cur_waiter == waiter)
                parent.cur_waiter = parent.waiters.erase(waiter);
            else
                parent.waiters.erase(waiter);
        }
        else if (was_waiter && will_be_waiter && new_competing_limit > old_limit)
        {
            need_schedule = true;
        }

        if (new_competing_limit > old_limit)
        {
            // See RR setMax comment for the baseline rationale.
            const SlotCount baseline = std::max(old_limit, allocated);
            if (new_competing_limit > baseline)
            {
                const SlotCount delta = new_competing_limit - baseline;
                const SlotCount available = parent.state.available(lock);
                emitDelayedOnGrow(query_counted, delta, available);
            }
        }
    }
    if (need_schedule)
        parent.parent.schedule(lock);
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
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this(), false, last_slot_id++)); // can't use std::make_shared due to private ctor
        }
    }

    value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this(), true, last_slot_id++)); // can't use std::make_shared due to private ctor
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
}

void ConcurrencyControlMaxMinFairScheduler::removeWaiterLocked(Allocation * allocation)
{
    waiters.erase(waiters.iterator_to(*allocation));
}

SlotAllocationPtr ConcurrencyControlMaxMinFairScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    SlotCount limit = max - min;
    SlotCount granted = 0;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    auto allocation = SlotAllocationPtr(new Allocation(*this, min, max, granted, next_sequence_number++));

    if (granted < limit)
    {
        auto * a = static_cast<Allocation*>(allocation.get());
        addWaiterLocked(a);
    }
    // See RR allocate for the rationale. MMF's competing demand is `max - min` (= limit).
    if (limit > 0)
    {
        const SlotCount available = state.available(lock);
        emitDelayedOnGrow(static_cast<Allocation *>(allocation.get())->query_counted, limit, available);
    }
    parent.schedule(lock);
    return allocation;
}

void ConcurrencyControlMaxMinFairScheduler::free(Allocation * allocation)
{
    std::unique_lock lock{state.mutex};
    auto [amount, is_waiting] = allocation->cancel();

    state.cur_concurrency -= amount;
    if (is_waiting)
        removeWaiterLocked(allocation);
    parent.schedule(lock);
}

void ConcurrencyControlMaxMinFairScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock);
}

// Max-min fair: walk waiters in (allocated, sequence) order and grant up to each one's
// current `limit` while capacity allows. The sort key changes on grant (`allocated++`), so
// we erase before `grant` and re-insert if still a waiter.
void ConcurrencyControlMaxMinFairScheduler::schedule(std::unique_lock<std::mutex> &)
{
    while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
    {
        state.cur_concurrency++;

        auto it = waiters.begin();
        Allocation & allocation = *it;

        removeWaiterLocked(&allocation);
        chassert(!allocation.waiters_hook.is_linked());

        bool still_waiter = allocation.grant();
        if (still_waiter)
            addWaiterLocked(&allocation);
    }
}

// See ConcurrencyControlRoundRobinScheduler::Allocation::setMax for contract.
void ConcurrencyControlMaxMinFairScheduler::Allocation::setMax(SlotCount new_max)
{
    chassert(new_max > 0);
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
            need_schedule = true;
        }

        if (new_competing_limit > old_limit)
        {
            // See RR setMax comment for the baseline rationale.
            const SlotCount baseline = std::max(old_limit, allocated);
            if (new_competing_limit > baseline)
            {
                const SlotCount delta = new_competing_limit - baseline;
                const SlotCount available = parent.state.available(lock);
                emitDelayedOnGrow(query_counted, delta, available);
            }
        }
    }
    if (need_schedule)
        parent.parent.schedule(lock);
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

void ConcurrencyControl::schedule(std::unique_lock<std::mutex> & lock)
{
    // Run all three schedulers each round so transitions (setScheduler) leave behind no
    // stranded waiters in the previous mode.
    switch (scheduler)
    {
        case Scheduler::RoundRobin:
            fair_round_robin.schedule(lock);
            max_min_fair.schedule(lock);
            round_robin.schedule(lock);
            return;
        case Scheduler::FairRoundRobin:
            round_robin.schedule(lock);
            max_min_fair.schedule(lock);
            fair_round_robin.schedule(lock);
            return;
        case Scheduler::MaxMinFair:
            round_robin.schedule(lock);
            fair_round_robin.schedule(lock);
            max_min_fair.schedule(lock);
            return;
    }
}

}
