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


ConcurrencyControlRoundRobinScheduler::Slot::Slot(SlotAllocationPtr && allocation_)
    : allocation(std::move(allocation_))
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
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this())); // can't use std::make_shared due to private ctor
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

SlotAllocationPtr ConcurrencyControlRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Try allocate slots up to requested `max` (as availability allows)
    SlotCount granted = std::max(min, std::min(max, state.available(lock)));
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation and start waiting if more slots are required
    if (granted < max)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, max - granted);
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        return SlotAllocationPtr(new Allocation(*this, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
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
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    parent.schedule(lock);
}

void ConcurrencyControlRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock);
}

// Round-robin scheduling of available slots among waiting allocations
void ConcurrencyControlRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &)
{
    while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
    {
        state.cur_concurrency++;
        if (cur_waiter == waiters.end())
            cur_waiter = waiters.begin();
        Allocation * allocation = *cur_waiter;
        if (allocation->grant())
            ++cur_waiter;
        else
            cur_waiter = waiters.erase(cur_waiter); // last required slot has just been granted -- stop waiting
    }
}


ConcurrencyControlFairRoundRobinScheduler::Slot::Slot(SlotAllocationPtr && allocation_, bool competing_)
    : allocation(std::move(allocation_))
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
            std::unique_lock lock{mutex};
            return AcquiredSlotPtr(new Slot(shared_from_this(), false)); // can't use std::make_shared due to private ctor
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
            return AcquiredSlotPtr(new Slot(shared_from_this(), true)); // can't use std::make_shared due to private ctor
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
{
}

ConcurrencyControlFairRoundRobinScheduler::~ConcurrencyControlFairRoundRobinScheduler()
{
    if (!waiters.empty())
        abort();
}

SlotAllocationPtr ConcurrencyControlFairRoundRobinScheduler::allocate(std::unique_lock<std::mutex> & lock, SlotCount min, SlotCount max)
{
    // Try allocate slots up to requested `max - min` (as availability allows).
    // Do not count `min` slots towards the limit. They are NOT considered as taking part in competition.
    SlotCount limit = max - min;
    SlotCount granted = std::min(limit, state.available(lock));
    state.cur_concurrency += granted;
    ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsGranted, min);

    // Create allocation and start waiting if more slots are required
    if (granted < limit)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsDelayed, limit - granted);
        ProfileEvents::increment(ProfileEvents::ConcurrencyControlQueriesDelayed);
        return SlotAllocationPtr(new Allocation(*this, min, max, granted,
            waiters.insert(cur_waiter, nullptr /* pointer is set by Allocation ctor */)));
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
    if (waiter)
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    parent.schedule(lock);
}

void ConcurrencyControlFairRoundRobinScheduler::release(SlotCount amount)
{
    std::unique_lock lock{state.mutex};
    state.cur_concurrency -= amount;
    parent.schedule(lock);
}

// Round-robin scheduling of available slots among waiting allocations
void ConcurrencyControlFairRoundRobinScheduler::schedule(std::unique_lock<std::mutex> &)
{
    while (!waiters.empty() && state.cur_concurrency < state.max_concurrency)
    {
        state.cur_concurrency++;
        if (cur_waiter == waiters.end())
            cur_waiter = waiters.begin();
        Allocation * allocation = *cur_waiter;
        if (allocation->grant())
            ++cur_waiter;
        else
            cur_waiter = waiters.erase(cur_waiter); // last required slot has just been granted -- stop waiting
    }
}


ConcurrencyControl::ConcurrencyControl()
    : round_robin(*this, state)
    , fair_round_robin(*this, state)
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
    return false; // invalid value - stick to the current scheduler
}

String ConcurrencyControl::getScheduler() const
{
    std::unique_lock lock{state.mutex};
    switch (scheduler)
    {
        case Scheduler::RoundRobin: return "round_robin";
        case Scheduler::FairRoundRobin: return "fair_round_robin";
    }
}

void ConcurrencyControl::schedule(std::unique_lock<std::mutex> & lock)
{
    switch (scheduler)
    {
        case Scheduler::RoundRobin:
            fair_round_robin.schedule(lock); // first schedule from old scheduler (works only during transition period)
            round_robin.schedule(lock);
            return;
        case Scheduler::FairRoundRobin:
            round_robin.schedule(lock); // first schedule from old scheduler (works only during transition period)
            fair_round_robin.schedule(lock);
            return;
    }
}

}
