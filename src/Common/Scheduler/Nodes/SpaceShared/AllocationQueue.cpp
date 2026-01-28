#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/IWorkloadNode.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int SERVER_OVERLOADED;
    extern const int QUERY_WAS_CANCELLED;
}

AllocationQueue::AllocationQueue(EventQueue & event_queue_, const SchedulerNodeInfo & info_, Int64 max_queued_)
    : IAllocationQueue(event_queue_, info_)
    , max_queued(max_queued_)
    , cancel_error(std::make_exception_ptr(Exception(ErrorCodes::QUERY_WAS_CANCELLED,"Allocation was cancelled")))
{}

AllocationQueue::~AllocationQueue()
{
    purgeQueue();
}

const String & AllocationQueue::getTypeName() const
{
    static String type_name("allocation_queue");
    return type_name;
}

void AllocationQueue::insertAllocation(ResourceAllocation & allocation, ResourceCost initial_size)
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
        ++rejects;
        throw Exception(ErrorCodes::SERVER_OVERLOADED,
            "Workload '{}' limit `max_waiting_queries` has been reached: {} of {}",
            getWorkloadName(), pending_allocations.size(), max_queued);
    }

    // Prepare allocation
    allocation.unique_id = ++last_unique_id;

    if (initial_size > 0) // Enqueue as a pending new allocation
    {
        allocation.increase.prepare(initial_size, IncreaseRequest::Kind::Pending);
        pending_allocations.push_back(allocation);
        pending_allocations_size += initial_size;
        if (&allocation == &*pending_allocations.begin() && increasing_allocations.empty()) // Only if it should be processed next
            scheduleActivation();
    }
    else // Zero-cost allocations are not blocked - enqueue into running allocations directly
    {
        allocation.fair_key = 0;
        running_allocations.insert(allocation);
    }
}

void AllocationQueue::increaseAllocation(ResourceAllocation & allocation, ResourceCost increase_size)
{
    chassert(increase_size > 0);

    std::lock_guard lock(mutex);
    ensureUsable();

    chassert(!allocation.increasing_hook.is_linked());

    // Update the key of running allocation
    running_allocations.erase(running_allocations.iterator_to(allocation));
    allocation.fair_key = allocation.allocated + increase_size;
    running_allocations.insert(allocation);

    // Enqueue increase request
    allocation.increase.prepare(increase_size, allocation.allocated == 0 ? IncreaseRequest::Kind::Initial : IncreaseRequest::Kind::Regular);
    increasing_allocations.insert(allocation);
    if (!skip_activation && &allocation == &*increasing_allocations.begin()) // Only if it should be processed next
        scheduleActivation();
}

void AllocationQueue::decreaseAllocation(ResourceAllocation & allocation, ResourceCost decrease_size)
{
    chassert(decrease_size > 0);

    std::lock_guard lock(mutex);
    chassert(!allocation.decreasing_hook.is_linked());
    if (allocation.running_hook.is_linked()) // Running allocation
    {
        allocation.decrease.prepare(decrease_size, decrease_size == allocation.allocated);
        decreasing_allocations.push_back(allocation);
        if (!skip_activation && &allocation == &*decreasing_allocations.begin()) // Only if it should be processed next (i.e. size = 1)
            scheduleActivation();
    }
    else // Special case - cancel pending allocation
    {
        // We cannot remove pending allocation here because it may be processed concurrently by the scheduler thread
        removing_allocations.push_back(allocation);
        if (&allocation == &*removing_allocations.begin())
            scheduleActivation();
    }
}

void AllocationQueue::purgeQueue()
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
    decreasing_allocations.clear();
    removing_allocations.clear();
    running_allocations.clear();

    // All further calls to this queue will throw exceptions
    increase = nullptr;
    decrease = nullptr;
    allocated = 0;
    allocations = 0;
    is_not_usable = true;
}

void AllocationQueue::propagateUpdate(ISpaceSharedNode &, Update &&)
{
    chassert(false);
}

void AllocationQueue::approveIncrease()
{
    std::lock_guard lock(mutex);
    chassert(increase);
    ResourceAllocation & allocation = increase->allocation;
    if (allocation.increase.kind == IncreaseRequest::Kind::Pending)
    {
        pending_allocations.erase(pending_allocations.iterator_to(allocation));
        pending_allocations_size -= allocation.increase.size;
        allocation.fair_key = increase->size;
        running_allocations.insert(allocation);
    }
    else
        increasing_allocations.erase(increasing_allocations.iterator_to(allocation));
    apply(*increase);
    allocation.allocated += increase->size;

    // Notify allocation
    skip_activation = true;
    increase->allocation.increaseApproved(*increase); // NOTE: this may re-enter increaseAllocation()
    skip_activation = false;
    increase = nullptr;

    setIncrease();
}

void AllocationQueue::approveDecrease()
{
    std::lock_guard lock(mutex);

    chassert(decrease);
    ResourceAllocation & allocation = decrease->allocation;
    decreasing_allocations.erase(decreasing_allocations.iterator_to(allocation));

    // We need to remove from running/increasing allocations to update the key
    running_allocations.erase(running_allocations.iterator_to(allocation));
    bool is_increasing = allocation.increasing_hook.is_linked();
    if (is_increasing)
        increasing_allocations.erase(increasing_allocations.iterator_to(allocation));

    // Update the key and other fields
    apply(*decrease);
    allocation.allocated -= decrease->size;
    allocation.fair_key -= decrease->size;

    // Reinsert into the appropriate data structures
    if (allocation.allocated > 0)
    {
        running_allocations.insert(allocation);
        if (is_increasing)
            increasing_allocations.insert(allocation);
    }

    // Ordering of increasing allocations is changed - update the next increase request if needed and propagate the update
    if (is_increasing && setIncrease())
        propagate(Update().setIncrease(increase));

    // Notify allocation
    skip_activation = true;
    decrease->allocation.decreaseApproved(*decrease); // NOTE: this may re-enter decreaseAllocation()
    skip_activation = false;
    decrease = nullptr;

    setDecrease();
}

ResourceAllocation * AllocationQueue::selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details)
{
    UNUSED(limit);

    // It is important not to kill allocation due to pending allocation in the same queue
    if (killer.kind == IncreaseRequest::Kind::Pending && &killer.allocation.queue == this)
        return nullptr;

    std::lock_guard lock(mutex);
    if (running_allocations.empty())
        return nullptr;

    // Kill the largest allocation. It is the last as the set is ordered by size.
    ResourceAllocation & victim = *running_allocations.rbegin();

    // If this is the least common ancestor of killer and victim - add details
    if (&killer.allocation.queue == this)
    {
        if (&killer.allocation == &victim)
            details = fmt::format("Evicting the largest allocation of size {} in workload '{}' to satisfy its own increase for {}.",
                getWorkloadName(), formatReadableCost(victim.allocated), formatReadableCost(killer.size));
        else
            details = fmt::format("Evicting the largest allocation of size {} in workload '{}' to satisfy increase of a smaller allocation.",
                getWorkloadName(), formatReadableCost(victim.allocated));
    }

    return &victim;
}

void AllocationQueue::processActivation()
{
    chassert(parent);
    Update update;
    {
        std::lock_guard lock(mutex);

        // Remove allocation if necessary
        while (!removing_allocations.empty())
        {
            ResourceAllocation & allocation = removing_allocations.front();
            removing_allocations.pop_front(); // Unlink before calling allocationFailed() to avoid use-after-free race
            if (allocation.pending_hook.is_linked()) // Allocation is still pending - cancel it
            {
                pending_allocations.erase(pending_allocations.iterator_to(allocation));
                pending_allocations_size -= allocation.increase.size;
                allocation.allocationFailed(cancel_error);
            }
            // else: allocation is now running - it is responsibility of allocation to decrease itself to zero in this case
        }

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

void AllocationQueue::attachChild(const SchedulerNodePtr &)
{
    throw Exception(
        ErrorCodes::INVALID_SCHEDULER_NODE,
        "Cannot add child to a leaf allocation queue: {}",
        getPath());
}

void AllocationQueue::removeChild(ISchedulerNode *)
{
}

ISchedulerNode * AllocationQueue::getChild(const String &)
{
    return nullptr;
}

std::pair<UInt64, Int64> AllocationQueue::getQueueLengthAndSize()
{
    std::lock_guard lock(mutex);
    return {pending_allocations.size(), pending_allocations_size};
}

void AllocationQueue::updateQueueLimit(Int64 value)
{
    std::lock_guard lock(mutex);
    max_queued = value;

    while (max_queued >= 0 && static_cast<size_t>(max_queued) < pending_allocations.size())
    {
        ResourceAllocation & allocation = pending_allocations.back();
        pending_allocations.erase(pending_allocations.iterator_to(allocation));
        pending_allocations_size -= allocation.increase.size;
        allocation.allocationFailed(std::make_exception_ptr(
            Exception(ErrorCodes::SERVER_OVERLOADED,
                "Workload '{}' limit `max_waiting_queries` has been reached: {} of {}",
                getWorkloadName(), pending_allocations.size(), max_queued)));
        ++rejects;
    }
}

bool AllocationQueue::setIncrease() // TSA_REQUIRES(mutex)
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

bool AllocationQueue::setDecrease() // TSA_REQUIRES(mutex)
{
    DecreaseRequest * old_decrease = decrease;
    if (!decreasing_allocations.empty())
        decrease = &decreasing_allocations.begin()->decrease;
    else
        decrease = nullptr;
    return old_decrease != decrease;
}

void AllocationQueue::ensureUsable() const // TSA_REQUIRES(mutex)
{
    if (is_not_usable)
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE,
        "Allocation queue is about to be destructed for workload '{}'",
        getWorkloadName());
}

UInt64 AllocationQueue::getRejects() const
{
    std::lock_guard lock(mutex);
    return rejects;
}

UInt64 AllocationQueue::getPending() const
{
    std::lock_guard lock(mutex);
    return pending_allocations.size();
}

}
