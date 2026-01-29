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
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    void processActivation() override;
    void attachChild(const SchedulerNodePtr &) override;
    void removeChild(ISchedulerNode *) override;
    ISchedulerNode * getChild(const String &) override;
    std::pair<UInt64, Int64> getQueueLengthAndSize();
    void updateQueueLimit(Int64 value);

    UInt64 getRejects() const;
    UInt64 getPending() const;

private:
    bool setIncrease();
    bool setDecrease();
    void ensureUsable() const;

    /// Protects all the following fields
    /// NOTE: we need recursive mutex because increaseApproved()/decreaseApproved() may interact with the queue again
    mutable std::recursive_mutex mutex;

    Int64 max_queued; /// Limit on the number of pending allocation

    bool is_not_usable = false; /// true after purgeQueue() to prevent new requests
    std::exception_ptr cancel_error; /// preallocated exception for cancelling requests

    ResourceAllocation::PendingList pending_allocations; /// Pending new allocations
    ResourceAllocation::RunningSet running_allocations; /// Currently running (not pending) allocations
    ResourceAllocation::IncreasingSet increasing_allocations; /// Allocations with pending increase request
    ResourceAllocation::DecreasingList decreasing_allocations; /// Allocations with pending decrease request
    ResourceAllocation::RemovingList removing_allocations; /// Allocations to remove

    size_t last_unique_id = 0;
    ResourceCost pending_allocations_size = 0;

    bool skip_activation = false; /// Optimization to avoid unnecessary activation

    UInt64 rejects = 0; /// Number of rejected allocations
};

}
