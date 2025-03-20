#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/ISlotControl.h>

#include <atomic>
#include <condition_variable>
#include <mutex>

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
    CpuSlotsAllocation(SlotCount master_slots_, SlotCount worker_slots_, ResourceLink master_link_, ResourceLink worker_link_);
    ~CpuSlotsAllocation() override;

    // Take one already granted slot if available. Lock-free iff there is no granted slot.
    [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

    // Blocks until there is a granted slot to acquire
    [[nodiscard]] AcquiredSlotPtr acquire() override;

private:
    friend class CpuSlotRequest; // for schedule() and failed()

    // Resource request failed
    void failed(const std::exception_ptr & ptr);

    // Enqueue resource request if necessary
    void grant();

    // Returns the queue for the current request
    ISchedulerQueue * getCurrentQueue(const std::unique_lock<std::mutex> &);

    const SlotCount master_slots; // Max number of slots to allocate using master link
    const SlotCount total_slots; // Total number of slots to allocate using both links
    const SlotCount noncompeting_slots; // Number of slots granted without links
    const ResourceLink master_link;
    const ResourceLink worker_link;

    static constexpr SlotCount exception_value = SlotCount(-1);
    std::atomic<SlotCount> noncompeting{0}; // allocated noncompeting slots left to acquire
    std::atomic<SlotCount> granted{0}; // allocated competing slots left to acquire
    std::atomic<size_t> last_acquire_index{0};

    // Field that require sync with the scheduler thread
    std::mutex schedule_mutex;
    std::condition_variable schedule_cv;
    std::exception_ptr exception;
    SlotCount allocated = 0; // Total allocated slots including already released
    size_t waiters = 0; // Number of threads waiting on acquire() call
    std::vector<CpuSlotRequest> requests; // Requests per every slot
    CpuSlotRequest * current_request;
};

}
