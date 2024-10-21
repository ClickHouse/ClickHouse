#pragma once

#include <limits>
#include <memory>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>


namespace DB
{

// Interfaces for abstract "slot" allocation and control.
// Slot is a virtual entity existing in a limited amount (CPUs or memory chunks, etc).
//
// Every slot can be in one of the following states:
//  * free: slot is available to be allocated.
//  * allocated: slot is allocated to a specific ISlotAllocation.
//
// Allocated slots can be in one of the following states:
//  * granted: allocated, but not yet acquired.
//  * acquired: a granted slot becomes acquired by using IAcquiredSlot.
//
// Example for CPU (see ConcurrencyControl.h). Every slot represents one CPU in the system.
// Slot allocation is a request to allocate specific number of CPUs for a specific query.
// Acquired slot is an entity that is held by a thread as long as it is running. This allows
// total number of threads in the system to be limited and the distribution process to be controlled.
//
// TODO:
// - for preemption - ability to return granted slot back and reacquire it later.
// - for memory allocations - variable size of slots (in bytes).

/// Number of slots
using SlotCount = UInt64;

/// Unlimited number of slots
constexpr SlotCount UnlimitedSlots = std::numeric_limits<SlotCount>::max();

/// Acquired slot holder. Slot is considered to be acquired as long as the object exists.
class IAcquiredSlot : public std::enable_shared_from_this<IAcquiredSlot>, boost::noncopyable
{
public:
    virtual ~IAcquiredSlot() = default;
};

using AcquiredSlotPtr = std::shared_ptr<IAcquiredSlot>;

/// Request for allocation of slots from ISlotControl.
/// Allows for more slots to be acquired and the whole request to be canceled.
class ISlotAllocation : public std::enable_shared_from_this<ISlotAllocation>, boost::noncopyable
{
public:
    virtual ~ISlotAllocation() = default;

    /// Take one already granted slot if available.
    [[nodiscard]] virtual AcquiredSlotPtr tryAcquire() = 0;

    /// Returns the number of granted slots for given allocation (i.e. available to be acquired)
    virtual SlotCount grantedCount() const = 0;

    /// Returns the total number of slots allocated at the moment (acquired and granted)
    virtual SlotCount allocatedCount() const = 0;
};

using SlotAllocationPtr = std::shared_ptr<ISlotAllocation>;

class ISlotControl : boost::noncopyable
{
public:
    virtual ~ISlotControl() = default;

    // Allocate at least `min` and at most `max` slots.
    // If not all `max` slots were successfully allocated, a "subscription" for later allocation is created
    [[nodiscard]] virtual SlotAllocationPtr allocate(SlotCount min, SlotCount max) = 0;
};

/// Allocation that grants all the slots immediately on creation
class GrantedAllocation : public ISlotAllocation
{
public:
    explicit GrantedAllocation(SlotCount granted_)
        : granted(granted_)
        , allocated(granted_)
    {}

    [[nodiscard]] AcquiredSlotPtr tryAcquire() override
    {
        SlotCount value = granted.load();
        while (value)
        {
            if (granted.compare_exchange_strong(value, value - 1))
                return std::make_shared<IAcquiredSlot>();
        }
        return {};
    }

    SlotCount grantedCount() const override
    {
        return granted.load();
    }

    SlotCount allocatedCount() const override
    {
        return allocated;
    }

private:
    std::atomic<SlotCount> granted; // allocated, but not yet acquired
    const SlotCount allocated;
};

[[nodiscard]] inline SlotAllocationPtr grantSlots(SlotCount count)
{
    return SlotAllocationPtr(new GrantedAllocation(count));
}

}
