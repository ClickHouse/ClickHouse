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
    enum class Kind
    {
        Regular, /// Regular increase request of running allocation
        Initial, /// The first increase request of a running allocation with zero size (e.g. due to `reserve_memory = 0`)
        Pending, /// The first increase request of a pending allocation (often handled differently than previous because query is not running yet)
    };

    /// Allocation associated with this request.
    ResourceAllocation & allocation;

    /// Allocation increase size.
    /// It must be greater than zero and remain constant until increaseApproved().
    ResourceCost size;

    /// When allocation is inserted into a queue, allocation should be increased from zero to its initial size.
    /// During this period allocation is pending (i.e. not yet running).
    Kind kind = Kind::Regular;

    explicit IncreaseRequest(ResourceAllocation & allocation_)
        : allocation(allocation_)
    {}

    void prepare(ResourceCost size_, Kind kind_)
    {
        size = size_;
        kind = kind_;
    }
};

}
