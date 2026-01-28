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
    explicit ResourceAllocation(IAllocationQueue & queue_, const String & id_ = {})
        : queue(queue_), id(id_), increase(*this), decrease(*this)
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

    IAllocationQueue & queue; /// Queue that manages this allocation.
    String const id; /// ID of this allocation for introspection purposes.

private:
    friend class AllocationQueue;

    ResourceCost allocated = 0; /// Currently allocated.

    IncreaseRequest increase;
    DecreaseRequest decrease;

    /// Hooks required for integration with AllocationQueue.
    boost::intrusive::list_member_hook<> pending_hook;
    boost::intrusive::set_member_hook<> running_hook;
    boost::intrusive::set_member_hook<> increasing_hook;
    boost::intrusive::list_member_hook<> decreasing_hook;
    boost::intrusive::list_member_hook<> removing_hook;
    using PendingHook    = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::list_member_hook<>, &ResourceAllocation::pending_hook>;
    using RunningHook    = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::set_member_hook<>, &ResourceAllocation::running_hook>;
    using IncreasingHook = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::set_member_hook<>, &ResourceAllocation::increasing_hook>;
    using DecreasingHook = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::list_member_hook<>, &ResourceAllocation::decreasing_hook>;
    using RemovingHook   = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::list_member_hook<>, &ResourceAllocation::removing_hook>;

    /// Keys for intrusive sets
    /// NOTE: Can only be accessed under queue.mutex as it is used in ordering, allocation.mutex is not needed.
    size_t unique_id = 0; /// Unique id for tie breaking in ordering.
    ResourceCost fair_key = 0; /// Currently allocated plus pending increase (key for max-min fair ordering).

    /// Ordering by size and unique id for tie breaking
    /// Used for both running and increasing allocations for consistent ordering
    /// NOTE: called outside of the scheduler thread and thus requires queue.mutex
    struct ByFairKey { bool operator()(const auto & lhs, const auto & rhs) const noexcept { return std::tie(lhs.fair_key, lhs.unique_id) < std::tie(rhs.fair_key, rhs.unique_id); } };

    /// Intrusive data structures for managing allocations
    /// We use intrusive structures to avoid allocations during scheduling (we might be under memory pressure)
    using PendingList    = boost::intrusive::list<ResourceAllocation, PendingHook>;
    using RunningSet     = boost::intrusive::set<ResourceAllocation, RunningHook, boost::intrusive::compare<ByFairKey>>;
    using IncreasingSet  = boost::intrusive::set<ResourceAllocation, IncreasingHook, boost::intrusive::compare<ByFairKey>>;
    using DecreasingList = boost::intrusive::list<ResourceAllocation, DecreasingHook>;
    using RemovingList   = boost::intrusive::list<ResourceAllocation, RemovingHook>;
};

}
