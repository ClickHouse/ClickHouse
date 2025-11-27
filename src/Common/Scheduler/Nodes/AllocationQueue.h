#pragma once

#include <Common/Scheduler/IAllocationQueue.h>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/options.hpp>

#include <mutex>
#include <exception>


namespace DB
{

/// Manages a set of resource allocations (e.g. memory reservations) for queries sharing the same workload.
///
/// Three types of requests that may be scheduled:
/// - increase: (a) running or (b) pending allocation (allocated=0)
/// - decrease: (c) running allocation
class AllocationQueue final : public IAllocationQueue
{
    static constexpr Int64 default_max_queued = std::numeric_limits<Int64>::max();
public:
    AllocationQueue(EventQueue & event_queue_, const SchedulerNodeInfo & info_, Int64 max_queued_ = default_max_queued);
    ~AllocationQueue() override;

    const String & getTypeName() const override;
    void insertAllocation(ResourceAllocation & allocation, ResourceCost initial_size) override;
    void increaseAllocation(ResourceAllocation & allocation, ResourceCost increase_size) override;
    void decreaseAllocation(ResourceAllocation & allocation, ResourceCost decrease_size) override;
    void purgeQueue() override;
    void propagateUpdate(ISpaceSharedNode &, Update &&) override;
    void approveIncrease() override;
    void approveDecrease() override;
    ResourceAllocation * selectAllocationToKill(IncreaseRequest * killer, ResourceCost limit) override;
    void processActivation() override;
    void attachChild(const SchedulerNodePtr &) override;
    void removeChild(ISchedulerNode *) override;
    ISchedulerNode * getChild(const String &) override;
    std::pair<UInt64, Int64> getQueueLengthAndSize();
    void updateQueueLimit(Int64 value);

private:
    bool setIncrease();
    bool setDecrease();
    void ensureUsable() const;

    /// Protects all the following fields
    /// NOTE: we need recursive mutex because increaseApproved()/decreaseApproved() may interact with the queue again
    std::recursive_mutex mutex;

    Int64 max_queued; /// Limit on the number of pending allocation

    bool is_not_usable = false; /// true after purgeQueue() to prevent new requests
    std::exception_ptr cancel_error; /// preallocated exception for cancelling requests

    /// Ordering by size and unique id for tie breaking
    /// Used for both running and increasing allocations for consistent ordering
    struct CompareAllocations
    {
        bool operator()(const ResourceAllocation & lhs, const ResourceAllocation & rhs) const noexcept
        {
            return AllocationQueue::compareAllocations(lhs, rhs); // because CompareAllocations is not a friend of Allocation
        }
    };

    static bool compareAllocations(const ResourceAllocation & lhs, const ResourceAllocation & rhs) noexcept
    {
        // Note that is called outside of the scheduler thread and thus requires mutex
        return std::tie(lhs.fair_key, lhs.unique_id) < std::tie(rhs.fair_key, rhs.unique_id);
    }

    /// Hooks for intrusive data structures
    using PendingHook    = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::list_member_hook<>, &ResourceAllocation::pending_hook>;
    using RunningHook    = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::set_member_hook<>, &ResourceAllocation::running_hook>;
    using IncreasingHook = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::set_member_hook<>, &ResourceAllocation::increasing_hook>;
    using DecreasingHook = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::list_member_hook<>, &ResourceAllocation::decreasing_hook>;
    using RemovingHook   = boost::intrusive::member_hook<ResourceAllocation, boost::intrusive::list_member_hook<>, &ResourceAllocation::removing_hook>;

    /// Intrusive data structures for managing allocations
    /// We use intrusive structures to avoid allocations during scheduling (we might be under memory pressure)
    using PendingList    = boost::intrusive::list<ResourceAllocation, PendingHook>;
    using RunningSet     = boost::intrusive::set<ResourceAllocation, RunningHook, boost::intrusive::compare<CompareAllocations>>;
    using IncreasingSet  = boost::intrusive::set<ResourceAllocation, IncreasingHook, boost::intrusive::compare<CompareAllocations>>;
    using DecreasingList = boost::intrusive::list<ResourceAllocation, DecreasingHook>;
    using RemovingList   = boost::intrusive::list<ResourceAllocation, RemovingHook>;

    PendingList pending_allocations; /// Pending new allocations
    RunningSet running_allocations; /// Currently running (not pending) allocations
    IncreasingSet increasing_allocations; /// Allocations with pending increase request
    DecreasingList decreasing_allocations; /// Allocations with pending decrease request
    RemovingList removing_allocations; /// Allocations to remove

    size_t last_unique_id = 0;
    ResourceCost pending_allocations_size = 0;

    bool skip_activation = false; /// Optimization to avoid unnecessary activation
};

}
