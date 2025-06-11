#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/ISlotControl.h>

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace DB
{

class CPUSlotsAllocation;

// Represents a resource request for a cpu slot for a single thread
class CPUSlotRequest final : public ResourceRequest
{
public:
    CPUSlotRequest() = default;
    ~CPUSlotRequest() override = default;

    /// Callback to trigger resource consumption.
    void execute() override;

    /// Callback to trigger an error in case if resource is unavailable.
    void failed(const std::exception_ptr & ptr) override;

    CPUSlotsAllocation * allocation = nullptr;
};

// Scoped guard for acquired cpu slot
class AcquiredCPUSlot final : public IAcquiredSlot
{
public:
    explicit AcquiredCPUSlot(SlotAllocationPtr && allocation_, CPUSlotRequest * request_);
    ~AcquiredCPUSlot() override;

private:
    SlotAllocationPtr allocation; // Hold allocation to ensure request is not destructed
    CPUSlotRequest * request; // Resource request to finalize in destructor or nullptr for non-competing slot
    CurrentMetrics::Increment acquired_slot_increment;
};

// Manages group of cpu slots and slot requests for a single thread group (query)
// It allocates slots by sending resource requests using given resource links:
// - First it sends `master_slots` number of requests using `master_link`
// - Then it sends `worker_slots` number of requests using `worker_link`
// It sends not more than 1 concurrent resource request at a time.
// If either provided link is empty, then corresponding slots are considered
// non-competing and `AcquiredCPUSlot` are provided w/o resource requests.
// PipelineExecutor uses 1 master thread and `max_threads - 1` worker threads
class CPUSlotsAllocation final : public ISlotAllocation
{
public:
    CPUSlotsAllocation(SlotCount master_slots_, SlotCount worker_slots_, ResourceLink master_link_, ResourceLink worker_link_);
    ~CPUSlotsAllocation() override;

    // Take one already granted slot if available. Lock-free iff there is no granted slot.
    [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

    // Blocks until there is a granted slot to acquire
    [[nodiscard]] AcquiredSlotPtr acquire() override;

    // For tests only. Returns true iff resource request is enqueued in the scheduler
    bool isRequesting() const;

private:
    friend class CPUSlotRequest; // for schedule() and failed()

    // Resource request failed
    void failed(const std::exception_ptr & ptr);

    // Enqueue resource request if necessary
    void grant();

    // Returns the queue for the current request
    ISchedulerQueue * getCurrentQueue(const std::unique_lock<std::mutex> &) const;

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
    mutable std::mutex schedule_mutex;
    std::condition_variable schedule_cv;
    std::exception_ptr exception;
    SlotCount allocated = 0; // Total allocated slots including already released
    size_t waiters = 0; // Number of threads waiting on acquire() call
    std::vector<CPUSlotRequest> requests; // Requests per every slot
    CPUSlotRequest * current_request;
    std::optional<CurrentMetrics::Increment> scheduled_slot_increment;
    std::optional<ProfileEvents::Timer> wait_timer;
};

}
