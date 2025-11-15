#pragma once

#include <Common/Scheduler/IAllocationQueue.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include "base/defines.h"

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/options.hpp>

#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int SERVER_OVERLOADED;
    extern const int QUERY_WAS_CANCELLED;
}

/// Manages a set of resource allocations (e.g. memory reservations) for queries sharing the same workload.
///
/// Three types of requests that may be scheduled:
/// - increase: (a) running or (b) pending allocation (allocated=0)
/// - decrease: (c) running allocation
class AllocationQueue final : public IAllocationQueue
{
    static constexpr Int64 default_max_queued = std::numeric_limits<Int64>::max();
public:
    AllocationQueue(EventQueue & event_queue_, const SchedulerNodeInfo & info_, Int64 max_queued_ = default_max_queued)
        : IAllocationQueue(event_queue_, info_)
        , max_queued(max_queued_)
        , cancel_error(std::make_exception_ptr(Exception(ErrorCodes::QUERY_WAS_CANCELLED,"Allocation was cancelled")))
    {}

    ~AllocationQueue() override
    {
        purgeQueue();
    }

    const String & getTypeName() const override
    {
        static String type_name("allocation_queue");
        return type_name;
    }

    void insertAllocation(ResourceAllocation & allocation, ResourceCost initial_size) override
    {
        chassert(&allocation.queue == this);

        std::lock_guard lock(mutex);

        /// Validations
        ensureUsable();
        if (initial_size < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Negative allocation is not allowed: {}", initial_size);
        if (initial_size > 0 && max_queued >= 0 && pending_allocations.size() >= static_cast<size_t>(max_queued))
        {
            // rejected_requests++; // TODO(serxa): introspection counters
            // rejected_cost += allocation.cost;
            throw Exception(ErrorCodes::SERVER_OVERLOADED,
                "Workload limit `max_waiting_queries` has been reached: {} of {}",
                pending_allocations.size(), max_queued);
        }

        // Prepare allocation
        allocation.unique_id = ++last_unique_id;

        if (initial_size > 0) // Enqueue as a pending new allocation
        {
            allocation.increase.prepare(initial_size, true);
            pending_allocations.push_back(allocation);
            pending_allocations_size += initial_size;
            if (&allocation == &*pending_allocations.begin() && increasing_allocations.empty()) // Only if it should be processed next
                scheduleActivation();
        }
        else // Zero-cost allocations are not blocked - enqueue into running allocations directly
            running_allocations.insert(allocation);
    }

    void increaseAllocation(ResourceAllocation & allocation, ResourceCost increase_size) override
    {
        chassert(increase_size > 0);

        std::lock_guard lock(mutex);
        ensureUsable();

        chassert(!allocation.increasing_hook.is_linked());
        allocation.increase.prepare(increase_size, false);
        increasing_allocations.insert(allocation);
        if (&allocation == &*increasing_allocations.begin()) // Only if it should be processed next
            scheduleActivation();
    }

    void decreaseAllocation(ResourceAllocation & allocation, ResourceCost decrease_size) override
    {
        chassert(decrease_size > 0);

        std::lock_guard lock(mutex);
        chassert(!allocation.decreasing_hook.is_linked());
        if (allocation.running_hook.is_linked()) // Running allocation
            decreaseRunningAllocation(allocation, decrease_size);
        else // Special case - cancel pending allocation
        {
            // We cannot remove pending allocation here because it may be processed concurrently by the scheduler thread
            removing_allocations.push_back(allocation);
            if (&allocation == &*removing_allocations.begin())
                scheduleActivation();
        }
    }

    void purgeQueue() override
    {
        std::lock_guard lock(mutex);

        /// Only detached queue can be purged to keep parents consistent it also means there is no scheduled activation
        chassert(parent == nullptr);

        // Fail all allocations
        for (ResourceAllocation & allocation : pending_allocations)
            allocation.allocationFailed(std::make_exception_ptr(
                Exception(ErrorCodes::INVALID_SCHEDULER_NODE,
                    "Queue for pending allocation is about to be destructed")));

        // NOTE: Queue never owns allocations, so they are not destructed here, just detached
        pending_allocations.clear();
        increasing_allocations.clear();
        decreasing_allocations.clear(); // Ignore them, we never fail decrease requests
        removing_allocations.clear();
        running_allocations.clear();

        // All further calls to this queue will throw exceptions
        increase = nullptr;
        decrease = nullptr;
        allocated = 0;
        is_not_usable = true;
    }

    void attachChild(const SchedulerNodePtr &) override
    {
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Cannot add child to a leaf allocation queue: {}",
            getPath());
    }

    void removeChild(ISchedulerNode *) override
    {
    }

    ISchedulerNode * getChild(const String &) override
    {
        return nullptr;
    }

    ResourceAllocation * selectAllocationToKill() override
    {
        std::lock_guard lock(mutex);
        if (running_allocations.empty())
            return nullptr;
        // Kill the largest allocation. It is the last as the set is ordered by size.
        return &*running_allocations.rbegin();
    }
    void approveIncrease() override
    {
        std::lock_guard lock(mutex);
        applyIncrease();
        setIncrease();
    }

    void approveDecrease() override
    {
        std::lock_guard lock(mutex);
        applyDecrease();
        setDecrease();
    }

    void processActivation() override
    {
        chassert(parent);
        Update update;
        {
            std::lock_guard lock(mutex);

            // Remove allocation if necessary
            for (auto & allocation : removing_allocations)
            {
                if (allocation.pending_hook.is_linked()) // Allocation is still pending - cancel it
                {
                    pending_allocations.erase(pending_allocations.iterator_to(allocation));
                    pending_allocations_size -= allocation.increase.size;
                    allocation.allocationFailed(cancel_error);
                }
                else if (allocation.running_hook.is_linked()) // Allocation is already running - retry removal
                    decreaseRunningAllocation<true>(allocation, 0);
                else // Allocation was already removed
                    continue;
            }
            removing_allocations.clear();

            // Update requests
            if (setIncrease())
                update.setIncrease(increase);
            if (setDecrease())
                update.setDecrease(decrease);
        }

        // Propagate update to parent
        if (update)
            propagate(std::move(update));
    }

    void propagateUpdate(ISpaceSharedNode &, Update &&) override
    {
        chassert(false);
    }

    std::pair<UInt64, Int64> getQueueLengthAndSize()
    {
        std::lock_guard lock(mutex);
        return {pending_allocations.size(), pending_allocations_size};
    }

    void updateQueueLimit(Int64 value)
    {
        // TODO(serxa): implement
        UNUSED(value);
    }

private:
    template <bool from_activation = false>
    void decreaseRunningAllocation(ResourceAllocation & allocation, ResourceCost decrease_size) // TSA_REQUIRES(mutex)
    {
        allocation.decrease.prepare(decrease_size, decrease_size == allocation.allocated);
        decreasing_allocations.push_back(allocation);
        if constexpr (!from_activation)
            if (&allocation == &*decreasing_allocations.begin()) // Only if it should be processed next (i.e. size = 1)
                scheduleActivation();
    }

    void applyIncrease() // TSA_REQUIRES(mutex)
    {
        chassert(increase);
        ResourceAllocation & allocation = increase->allocation;
        if (allocation.allocated == 0)
        {
            pending_allocations.erase(pending_allocations.iterator_to(allocation));
            pending_allocations_size -= allocation.increase.size;
        }
        else
        {
            increasing_allocations.erase(increasing_allocations.iterator_to(allocation));
            // We need to remove from running allocations to update the key
            running_allocations.erase(running_allocations.iterator_to(allocation));
        }
        allocated += increase->size;
        allocation.allocated += increase->size;
        running_allocations.insert(allocation);
        increase->allocation.increaseApproved(*increase); // NOTE: this may re-enter increaseAllocation()
        increase = nullptr;
    }

    void applyDecrease() // TSA_REQUIRES(mutex)
    {
        chassert(decrease);
        ResourceAllocation & allocation = decrease->allocation;
        decreasing_allocations.erase(decreasing_allocations.iterator_to(allocation));
        // We need to remove from running allocations to update the key
        running_allocations.erase(running_allocations.iterator_to(allocation));
        bool is_increasing = allocation.increasing_hook.is_linked();
        if (is_increasing) // remove to update the key
            increasing_allocations.erase(increasing_allocations.iterator_to(allocation));
        allocated -= decrease->size;
        allocation.allocated -= decrease->size;
        if (allocation.allocated > 0)
        {
            running_allocations.insert(allocation);
            if (is_increasing)
                increasing_allocations.insert(allocation);
        }
        if (is_increasing)
        {
            // Ordering of increasing allocations is changed - update the next increase request if needed and propagate the update
            if (setIncrease())
                propagate(Update().setIncrease(increase));
        }
        decrease->allocation.decreaseApproved(*decrease); // NOTE: this may re-enter decreaseAllocation()
        decrease = nullptr;
    }

    bool setIncrease() // TSA_REQUIRES(mutex)
    {
        IncreaseRequest * old_increase = increase;
        if (!increasing_allocations.empty())
            increase = &increasing_allocations.begin()->increase;
        else if (!pending_allocations.empty())
            increase = &pending_allocations.begin()->increase;
        else
            increase = nullptr;
        return increase != old_increase;
    }

    bool setDecrease() // TSA_REQUIRES(mutex)
    {
        DecreaseRequest * old_decrease = decrease;
        if (!decreasing_allocations.empty())
            decrease = &decreasing_allocations.begin()->decrease;
        else
            decrease = nullptr;
        return old_decrease != decrease;
    }

    void ensureUsable() const // TSA_REQUIRES(mutex)
    {
        if (is_not_usable)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE,
            "Allocation queue is about to be destructed");
    }

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
        return std::tie(lhs.allocated, lhs.unique_id) < std::tie(rhs.allocated, rhs.unique_id);
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
};

}
