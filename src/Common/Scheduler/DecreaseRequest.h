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
/// NOTE: Calling queue methods from `decreaseApproved` is not allowed because it is called
/// during scheduler hierarchy traversal.
class DecreaseRequest final
{
public:
    /// Allocation associated with this request.
    ResourceAllocation & allocation;

    /// Allocation decrease size.
    ResourceCost size = 0;

    /// Set only by `removeAllocation` (via `processActivation`) to signal that the allocation
    /// is being removed. Normal decreases never set this — an allocation may stay alive at zero.
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
