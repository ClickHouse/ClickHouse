#pragma once

#include <condition_variable>
#include <Common/Scheduler/IncreaseRequest.h>
#include <Common/Scheduler/DecreaseRequest.h>

#include <boost/core/noncopyable.hpp>
#include <boost/intrusive/set_hook.hpp>
#include <boost/intrusive/list_hook.hpp>

namespace DB
{

class ISpaceSharedNode;
class IAllocationQueue;
class AllocationQueue;

/// Represents a resource allocation that could change its size during its lifetime.
/// Both increase and decrease of size are done through interaction with IAllocationQueue.
/// The first increase is from zero size (i.e. new allocation).
/// The last decrease is to zero size (i.e. free allocation).
/// Only one pending request of each type is allowed at a time.
/// When resource is exhausted, scheduler may kill the allocation.
class ResourceAllocation : public boost::noncopyable
{
public:
    ResourceAllocation(
        IAllocationQueue & queue_,
        ResourceCost initial_size,
        IncreaseRequest & increase_,
        DecreaseRequest & decrease_);

    virtual ~ResourceAllocation();

    /// Scheduler asks this allocation to be completely released.
    /// IMPORTANT: it is called from the scheduler thread and must be fast,
    /// just triggering procedure, not doing the kill itself.
    virtual void killAllocation(const std::exception_ptr & reason) = 0;

    /// Scheduler threads calls this method when queue is about to be destructed.
    virtual void onQueuePurge() = 0;

    /// Queue that manages this allocation.
    IAllocationQueue & queue;

protected:
    /// Allocation is completely removed from the queue, required before destruction.
    /// Derived classes must call this method when allocation is fully removed:
    /// - due to failed increase request of a pending allocation
    /// - or due to decrease to zero size of a running allocation (check removing_allocation flag)
    void allocationRemoved();

    std::mutex mutex;
    std::condition_variable cv;
    bool is_removed = false;

private:
    friend class AllocationQueue; // TODO(serxa): move the following fields to a separate struct to better hide implementation details?

    IncreaseRequest & increase;
    DecreaseRequest & decrease;

    /// Hooks required for integration with AllocationQueue.
    boost::intrusive::list_member_hook<> pending_hook;
    boost::intrusive::set_member_hook<> running_hook;
    boost::intrusive::set_member_hook<> increasing_hook;
    boost::intrusive::list_member_hook<> decreasing_hook;
    boost::intrusive::list_member_hook<> removing_hook;

    /// Unique id for tie breaking in ordering.
    /// NOTE: Can only be accessed under AllocationQueue::mutex as it is used in ordering.
    size_t unique_id = 0;

    /// Currently allocated.
    /// NOTE: Can only be accessed under AllocationQueue::mutex as it is used in ordering.
    ResourceCost allocated = 0;
};

}
