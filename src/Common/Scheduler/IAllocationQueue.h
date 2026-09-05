#pragma once

#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/ISpaceSharedNode.h>

namespace DB
{

/// Queue used for holding pending and running resource allocations.
/// It is a leaf node of the scheduling hierarchy that queries interact with to create and manage their allocations.
class IAllocationQueue : public ISpaceSharedNode
{
public:
    IAllocationQueue(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
        : ISpaceSharedNode(event_queue_, info_)
    {}

    /// Adds a new allocation to be managed by this queue.
    /// If `initial_size` is zero, the allocation starts running immediately.
    /// Otherwise the allocation is pending until the scheduler approves it
    /// via `ResourceAllocation::increaseApproved`.
    /// Throws an exception if the allocation is invalid or rejected.
    /// May call `ResourceAllocation::allocationFailed` if the allocation is rejected later.
    virtual void insertAllocation(ResourceAllocation & allocation, ResourceCost initial_size) = 0;

    /// Requests to increase the size of a running allocation by `increase_size`.
    /// The request is processed by the scheduler thread.
    /// On approval `ResourceAllocation::increaseApproved` will be called.
    /// `increase_size` must be positive.
    virtual void increaseAllocation(ResourceAllocation & allocation, ResourceCost increase_size) = 0;

    /// Requests to decrease the size of a running allocation by `decrease_size`.
    /// The request is processed by the scheduler thread.
    /// On approval `ResourceAllocation::decreaseApproved` will be called.
    /// An allocation may stay alive at zero — use `removeAllocation` to remove it.
    /// `decrease_size` must be positive.
    virtual void decreaseAllocation(ResourceAllocation & allocation, ResourceCost decrease_size) = 0;

    /// Reports the amount of this allocation's size that can be spilled or discarded on request.
    /// `reclaimable_total` is an absolute total (not a delta) and is clamped to the current size.
    /// Advisory: never blocks and never fails. The per-subtree `reclaimable` aggregate is updated on the
    /// scheduler thread and propagated to the root; the scheduler uses it to choose spill victims.
    virtual void setReclaimable(ResourceAllocation & allocation, ResourceCost reclaimable_total) = 0;

    /// The reply to `ResourceAllocation::spillAllocation`: the query finished handling the spill request.
    /// Reports the remaining reclaimable total like `setReclaimable` (0 means nothing was or can be
    /// reclaimed, i.e. a decline) and additionally reopens the spill gate of every `AllocationLimit` above,
    /// allowing the scheduler to signal the next victim if the subtree is still over its soft limit.
    /// Call it AFTER issuing the decreases for the freed memory, so that the scheduler re-evaluates the
    /// soft limit only once the released memory is reflected in `allocated`.
    /// Never blocks and never fails.
    virtual void finishSpill(ResourceAllocation & allocation, ResourceCost reclaimable_total) = 0;

    /// Requests to remove an allocation from the queue.
    /// The removal is processed asynchronously by the scheduler thread.
    /// For pending allocations, `ResourceAllocation::allocationFailed` will be called.
    /// For running allocations, any pending increase is cancelled and a decrease to zero is enqueued
    /// (if a decrease is already pending, it is updated in-place to become a removal).
    /// On completion `ResourceAllocation::decreaseApproved` (with `removing_allocation=true`) will be called.
    virtual void removeAllocation(ResourceAllocation & allocation) = 0;

    /// Kills all the resource allocations in the queue, fails all pending requests
    /// and marks the queue as unusable. Any further calls will throw an exception.
    /// Can only be called from the scheduler thread.
    virtual void purgeQueue() = 0;
};

}
