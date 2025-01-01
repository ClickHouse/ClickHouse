#include <Common/ISlotControl.h>
#include <Common/ConcurrencyControl.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event ConcurrencyControlSlotsGranted;
    extern const Event ConcurrencyControlSlotsDelayed;
    extern const Event ConcurrencyControlSlotsAcquired;
    extern const Event ConcurrencyControlQueriesDelayed;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlSoftLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ConcurrencyControl::Slot::~Slot()
{
    static_cast<ConcurrencyControl::Allocation&>(*allocation).release();
}

ConcurrencyControl::Slot::Slot(SlotAllocationPtr && allocation_)
    : allocation(std::move(allocation_))
    , acquired_slot_increment(CurrentMetrics::ConcurrencyControlAcquired)
{
}

ConcurrencyControl::Allocation::~Allocation()
{
    // We have to lock parent's mutex to avoid race with grant()
    // NOTE: shortcut can be added, but it requires Allocation::mutex lock even to check if shortcut is possible
    parent.free(this);
}

[[nodiscard]] AcquiredSlotPtr ConcurrencyControl::Allocation::tryAcquire()
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

SlotCount ConcurrencyControl::Allocation::grantedCount() const
{
    return granted.load();
}

SlotCount ConcurrencyControl::Allocation::allocatedCount() const
{
    std::unique_lock lock{mutex};
    return allocated;
}

ConcurrencyControl::Allocation::Allocation(ConcurrencyControl & parent_, SlotCount limit_, SlotCount granted_, Waiters::iterator waiter_)
    : parent(parent_)
    , limit(limit_)
    , allocated(granted_)
    , granted(granted_)
    , waiter(waiter_)
{
    if (allocated < limit)
        *waiter = this;
}

// Grant single slot to allocation returns true iff more slot(s) are required
bool ConcurrencyControl::Allocation::grant()
{
    std::unique_lock lock{mutex};
    granted++;
    allocated++;
    return allocated < limit;
}

// Release one slot and grant it to other allocation if required
void ConcurrencyControl::Allocation::release()
{
    parent.release(1);
    std::unique_lock lock{mutex};
    released++;
    if (released > allocated)
        abort();
}

ConcurrencyControl::ConcurrencyControl()
    : cur_waiter(waiters.end())
    , max_concurrency_metric(CurrentMetrics::ConcurrencyControlSoftLimit, 0)
{
}

ConcurrencyControl::~ConcurrencyControl()
{
    if (!waiters.empty())
        abort();
}

[[nodiscard]] SlotAllocationPtr ConcurrencyControl::allocate(SlotCount min, SlotCount max)
{
    if (min > max)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ConcurrencyControl: invalid allocation requirements");

    std::unique_lock lock{mutex};

    // Acquire as many slots as we can, but not lower than `min`
    SlotCount granted = std::max(min, std::min(max, available(lock)));
    cur_concurrency += granted;
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
        return SlotAllocationPtr(new Allocation(*this, max, granted));
}

void ConcurrencyControl::setMaxConcurrency(SlotCount value)
{
    std::unique_lock lock{mutex};
    max_concurrency = std::max<SlotCount>(1, value); // never allow max_concurrency to be zero
    max_concurrency_metric.changeTo(max_concurrency == UnlimitedSlots ? 0 : max_concurrency);
    schedule(lock);
}

ConcurrencyControl & ConcurrencyControl::instance()
{
    static ConcurrencyControl result;
    return result;
}

void ConcurrencyControl::free(Allocation * allocation)
{
    // Allocation is allowed to be canceled even if there are:
    //  - `amount`: granted slots (acquired slots are not possible, because Slot holds AllocationPtr)
    //  - `waiter`: active waiting for more slots to be allocated
    // Thus Allocation destruction may require the following lock, to avoid race conditions
    std::unique_lock lock{mutex};
    auto [amount, waiter] = allocation->cancel();

    cur_concurrency -= amount;
    if (waiter)
    {
        if (cur_waiter == *waiter)
            cur_waiter = waiters.erase(*waiter);
        else
            waiters.erase(*waiter);
    }
    schedule(lock);
}

void ConcurrencyControl::release(SlotCount amount)
{
    std::unique_lock lock{mutex};
    cur_concurrency -= amount;
    schedule(lock);
}

// Round-robin scheduling of available slots among waiting allocations
void ConcurrencyControl::schedule(std::unique_lock<std::mutex> &)
{
    while (cur_concurrency < max_concurrency && !waiters.empty())
    {
        cur_concurrency++;
        if (cur_waiter == waiters.end())
            cur_waiter = waiters.begin();
        Allocation * allocation = *cur_waiter;
        if (allocation->grant())
            ++cur_waiter;
        else
            cur_waiter = waiters.erase(cur_waiter); // last required slot has just been granted -- stop waiting
    }
}

SlotCount ConcurrencyControl::available(std::unique_lock<std::mutex> &) const
{
    if (cur_concurrency < max_concurrency)
        return max_concurrency - cur_concurrency;
    return 0;
}

}
