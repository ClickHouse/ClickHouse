#pragma once

#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/ISpaceSharedNode.h>

namespace DB
{

/// Queue used for holding pending and existing resource allocations.
/// It is a leaf node of scheduling hierarchy, that queries interact with to create and manage their allocations.
class IAllocationQueue : public ISpaceSharedNode
{
public:
    IAllocationQueue(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
        : ISpaceSharedNode(event_queue_, info_)
    {}

    /// Adds new allocation to be managed by this queue.
    /// If `initial_size` is zero, allocation is approved immediately.
    /// Otherwise allocation is pending until ResourceAllocation::increase::execute() call.
    /// It throws exception if allocation is invalid or rejected.
    /// May return an error through ResourceAllocation::failed() callback if allocation is rejected later.
    virtual void insertAllocation(ResourceAllocation & allocation, ResourceCost initial_size) = 0;

    /// Requests to increase the size of an existing allocation by `increase_size`.
    /// Note that this request should be executed by the scheduler thread to be approved.
    /// On approval allocation.increaseApproved() will be called.
    /// If increase is not possible, allocation.failed() will be called.
    /// `increase_size` must be positive.
    virtual void increaseAllocation(ResourceAllocation & allocation, ResourceCost increase_size) = 0;

    /// Requests to decrease the size of an existing allocation by `decrease_size`.
    /// Note that this request should be executed by the scheduler thread.
    /// To confirm the decrease allocation.decreaseApproved() will be called.
    /// If allocation is decreased to zero it is automatically removed from the queue.
    /// If a pending allocation is decreased (to remove) an allocation.failed() will be called.
    /// `decrease_size` must be positive.
    virtual void decreaseAllocation(ResourceAllocation & allocation, ResourceCost decrease_size) = 0;

    /// Kill all the resource allocations in queue, fails all the pending requests and marks this queue as not usable.
    /// Afterwards on any new allocation or requests an exception will be thrown.
    /// Can only be called from the scheduler thread.
    virtual void purgeQueue() = 0;
};

}
