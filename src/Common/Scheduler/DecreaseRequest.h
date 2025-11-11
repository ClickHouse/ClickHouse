#pragma once

#include <Common/Scheduler/CostUnit.h>

#include <base/types.h>

namespace DB
{

// Forward declarations
class ResourceAllocation;

/// Request to decrease the resource allocation of a space-shared resource.
///
/// ----1=2222222222222=3----> time
///     ^       ^       ^
///  decrease  wait  approve
///
/// 1) Request is reset and allocation is enqueued using IAllocationQueue::decreaseAllocation().
/// 2) Request is waiting to be processed by the scheduler thread.
/// 3) Whenever request is approved by the scheduler, DecreaseRequest::execute() is called.
///
/// Every ResourceAllocation may have zero or one pending DecreaseRequest.
/// From execute() allocation is allowed to call IAllocationQueue::decreaseAllocation() again
/// to request further decrease of allocation if necessary.
class DecreaseRequest
{
public:
    // Allocation decrease size.
    ResourceCost size;

    /// Allocation associated with this request.
    ResourceAllocation * const allocation = nullptr;

    /// When allocation is being decreased to zero, it is automatically removed after this request is executed.
    bool removing_allocation = false;

    explicit DecreaseRequest(ResourceAllocation * allocation_, ResourceCost size_ = 1)
        : allocation(allocation_)
    {
        reset(size_);
    }

    /// The object may be reused again after reset().
    void reset(ResourceCost size_)
    {
        size = size_;
        removing_allocation = false;
    }

    virtual ~DecreaseRequest() = default;

    /// Callback to trigger on approval of the request.
    /// IMPORTANT: it is called from scheduler thread and must be fast.
    virtual void execute() = 0;
};

}
