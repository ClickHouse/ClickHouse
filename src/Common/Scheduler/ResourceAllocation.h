#pragma once

#include <exception>
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
    explicit ResourceAllocation(IAllocationQueue & queue_)
        : queue(queue_), increase(*this), decrease(*this)
    {}

    virtual ~ResourceAllocation();

    /// Previously reqeuested increase is approved. It is called from the scheduler thread.
    virtual void increaseApproved(const IncreaseRequest & increase) = 0;

    /// Previously reqeuested decrease is approved. It is called from the scheduler thread.
    virtual void decreaseApproved(const DecreaseRequest & decrease) = 0;

    /// Scheduler threads calls this method when queue is about to be destructed or pending allocation is cancelled.
    virtual void allocationFailed(const std::exception_ptr & reason) = 0;

    /// Scheduler asks this allocation to be completely released.
    /// IMPORTANT: it is called from the scheduler thread and must be fast,
    /// just triggering procedure, not doing the kill itself.
    virtual void killAllocation(const std::exception_ptr & reason) = 0;

    /// Queue that manages this allocation.
    IAllocationQueue & queue;

private:
    friend class AllocationQueue; // TODO(serxa): move the following fields to a separate struct to better hide implementation details?

    IncreaseRequest increase;
    DecreaseRequest decrease;

    /// Hooks required for integration with AllocationQueue.
    boost::intrusive::list_member_hook<> pending_hook;
    boost::intrusive::set_member_hook<> running_hook;
    boost::intrusive::set_member_hook<> increasing_hook;
    boost::intrusive::list_member_hook<> decreasing_hook;
    boost::intrusive::list_member_hook<> removing_hook;

    /// Unique id for tie breaking in ordering.
    /// NOTE: Can only be accessed under queue.mutex as it is used in ordering, allocation.mutex is not needed.
    size_t unique_id = 0;

    /// Currently allocated.
    /// NOTE: Can only be accessed under queue.mutex as it is used in ordering, allocation.mutex is not needed.
    ResourceCost allocated = 0;
};

}
