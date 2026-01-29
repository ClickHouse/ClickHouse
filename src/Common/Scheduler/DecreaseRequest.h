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
/// 1) Request is prepared and allocation is enqueued using IAllocationQueue::decreaseAllocation().
/// 2) Request is waiting to be processed by the scheduler thread.
/// 3) Whenever request is approved by the scheduler, ResourceAllocation::decreaseApproved() is called.
///
/// Every ResourceAllocation may have zero or one pending DecreaseRequest.
/// From decreaseApproved() allocation is allowed to call IAllocationQueue::decreaseAllocation() again
/// to request further decrease of allocation if necessary.
class DecreaseRequest final
{
public:
    /// Allocation associated with this request.
    ResourceAllocation & allocation;

    /// Allocation decrease size.
    /// It must be greater than zero and remain constant until decreaseApproved().
    ResourceCost size;

    /// When allocation is being decreased to zero, it is automatically removed after decreaseApproved().
    bool removing_allocation = false;

    explicit DecreaseRequest(ResourceAllocation & allocation_)
        : allocation(allocation_)
    {}

    void prepare(ResourceCost size_, bool removing_allocation_)
    {
        size = size_;
        removing_allocation = removing_allocation_;
    }
};

}
