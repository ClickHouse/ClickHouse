#pragma once

#include <atomic>
#include <mutex>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/ISlotControl.h>

namespace DB
{

class CpuSlotsAllocation;

// Represents a resource request for a cpu slot for a single thread
class CpuSlotRequest final : public ResourceRequest
{
public:
    CpuSlotRequest() = default;
    ~CpuSlotRequest() override = default;

    /// Callback to trigger resource consumption.
    void execute() override;

    /// Callback to trigger an error in case if resource is unavailable.
    void failed(const std::exception_ptr & ptr) override;

    CpuSlotsAllocation * allocation = nullptr;
};

// Scoped guard for acquired cpu slot
class AcquiredCpuSlot final : public IAcquiredSlot
{
public:
    explicit AcquiredCpuSlot(SlotAllocationPtr && allocation_, CpuSlotRequest * request_);
    ~AcquiredCpuSlot() override;

private:
    SlotAllocationPtr allocation; // Hold allocation to ensure request is not destructed
    CpuSlotRequest * request; // Resource request to finalize in destructor or nullptr for non-competing slot
    CurrentMetrics::Increment acquired_slot_increment;
};

// Manages group of cpu slots and slot requests for a single thread group (query)
class CpuSlotsAllocation final : public ISlotAllocation
{
public:
    CpuSlotsAllocation(SlotCount min_, SlotCount max, ResourceLink link);
    ~CpuSlotsAllocation() override;

    // Take one already granted slot if available. Lock-free iff there is no granted slot.
    [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

    SlotCount grantedCount() const override;
    SlotCount allocatedCount() const override;

private:
    friend class CpuSlotRequest; // for grant() and failed()

    // Grant single slot to allocation
    void grant();

    // Resource request failed
    void failed(const std::exception_ptr & ptr);

    // Enqueue resource request if necessary
    void schedule();

    const SlotCount min; // Count first `min` slots as NOT taking part in competition
    const SlotCount max;
    const ResourceLink link;

    static constexpr SlotCount exception_value = SlotCount(-1);
    std::atomic<SlotCount> noncompeting; // allocated noncompeting slots, but not yet acquired
    std::atomic<SlotCount> granted{0}; // allocated competing slots, but not yet acquired
    std::atomic<size_t> last_request_index{0};

    // Field that can be only accessed from the scheduler thread (and ctor/dtor)
    SlotCount allocated; // total allocated slots including already released

    // Requests per every slot
    // NOTE: it should not be reallocated after initialization because AcquiredCpuSlot holds raw pointer
    std::vector<CpuSlotRequest> requests;

    std::mutex exception_mutex;
    std::exception_ptr exception;
};

}
