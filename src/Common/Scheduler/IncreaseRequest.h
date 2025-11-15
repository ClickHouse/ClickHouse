#pragma once

#include <Common/Scheduler/CostUnit.h>

#include <base/types.h>

namespace DB
{

// Forward declarations
class ResourceAllocation;

/// Request to increase the resource allocation of a space-shared resource.
///
/// ----1=2222222222222=3----> time
///     ^       ^       ^
///  increase  wait  approve
///
/// 1) Request is prepared and allocation is enqueued using IAllocationQueue::increaseAllocation().
/// 2) Allocation competes with others for resource; effectively just waiting in a queue.
/// 3) Whenever request is approved by the scheduler, ResourceAllocation::increaseApproved() is called.
///
/// Every ResourceAllocation may have zero or one pending IncreaseRequest.
/// From increaseApproved() allocation is allowed to call IAllocationQueue::increaseAllocation() again
/// to request further increase of allocation if necessary.
class IncreaseRequest final
{
public:
    /// Allocation associated with this request.
    ResourceAllocation & allocation;

    /// Allocation increase size.
    /// It must be greater than zero and remain constant until increaseApproved().
    ResourceCost size;

    /// When allocation is inserted into a queue, allocation should be increased from zero to its initial size.
    /// During this period allocation is pending (i.e. not yet running).
    bool pending_allocation = false;

    explicit IncreaseRequest(ResourceAllocation & allocation_)
        : allocation(allocation_)
    {}

    void prepare(ResourceCost size_, bool pending_allocation_)
    {
        size = size_;
        pending_allocation = pending_allocation_;
    }
};

}
