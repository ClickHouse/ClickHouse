#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/IWorkloadNode.h>
#include <Common/Scheduler/Debug.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int RESOURCE_LIMIT_EXCEEDED;
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

std::string_view AllocationQueue::getTypeName() const { return "allocation_queue"; }

void AllocationQueue::insertAllocation(ResourceAllocation & allocation, ResourceCost initial_size)
{
    chassert(&allocation.queue == this);
    std::lock_guard lock(mutex);

    /// Validations
    ensureUsable();
    if (initial_size < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Negative allocation is not allowed: {}", initial_size);
    if (initial_size > min_max_allocated)
    {
        ++rejects;
        throw Exception(ErrorCodes::RESOURCE_LIMIT_EXCEEDED,
            "Workload '{}' allocation of size {} exceeds the limit of {}",
            getWorkloadName(), formatReadableCost(initial_size), formatReadableCost(min_max_allocated));
    }
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
        SCHED_DBG("{} -- insert(id={}, size={}, pending={})", basename, allocation.unique_id, initial_size, pending_allocations.size());
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

    // Enqueue increase request. `Kind::Initial` is the first increase that admits the allocation
    // into the hierarchy (it makes `apply(IncreaseRequest)` increment `allocations`). Use the
    // sticky `admitted` flag — not `allocated == 0` — because an allocation that has been admitted
    // and then shrunk back to zero must not be re-admitted on a later grow.
    allocation.increase.prepare(increase_size, allocation.admitted ? IncreaseRequest::Kind::Regular : IncreaseRequest::Kind::Initial);
    increasing_allocations.insert(allocation);
    if (&allocation == &*increasing_allocations.begin())
        scheduleActivation();
}

void AllocationQueue::decreaseAllocation(ResourceAllocation & allocation, ResourceCost decrease_size)
{
    chassert(decrease_size > 0);

    std::lock_guard lock(mutex);
    if (is_not_usable)
        return; // Queue has been purged — `allocationFailed` has already notified the owner.
    chassert(!allocation.decreasing_hook.is_linked());
    chassert(allocation.running_hook.is_linked());
    allocation.decrease.prepare(decrease_size, /*removing_allocation=*/ false);
    decreasing_allocations.push_back(allocation);
    if (&allocation == &*decreasing_allocations.begin())
        scheduleActivation();
}

void AllocationQueue::removeAllocation(ResourceAllocation & allocation)
{
    std::lock_guard lock(mutex);
    if (is_not_usable)
        return; // Queue has been purged — `allocationFailed` has already notified the owner.
    // If the allocation has been failed by a concurrent path (e.g. `updateMinMaxAllocated` or
    // `updateQueueLimit` rejected it after the owner's destructor checked `fail_reason` but
    // before this call), it is no longer in `pending_allocations` or `running_allocations`.
    // Adding it to `removing_allocations` in this state would leave `removing_hook` linked when
    // the owner reaches `~ResourceAllocation`, with `processActivation` later dereferencing a
    // freed object.
    if (!allocation.pending_hook.is_linked() && !allocation.running_hook.is_linked())
        return;
    removing_allocations.push_back(allocation);
    if (&allocation == &*removing_allocations.begin())
        scheduleActivation();
}

void AllocationQueue::purgeQueue()
{
    std::lock_guard lock(mutex);
    chassert(parent == nullptr);
    cancelActivation();

    auto reason = std::make_exception_ptr(
        Exception(ErrorCodes::INVALID_SCHEDULER_NODE,
            "Allocation queue is about to be destructed for workload '{}'",
            getWorkloadName()));

    // Fail allocation only after removing from all intrusive lists to avoid use-after-free
    while (!pending_allocations.empty())
    {
        ResourceAllocation & allocation = pending_allocations.front();
        pending_allocations.pop_front();
        pending_allocations_size -= allocation.increase.size;
        if (allocation.removing_hook.is_linked())
            removing_allocations.erase(removing_allocations.iterator_to(allocation));
        allocation.allocationFailed(reason);
    }

    while (!running_allocations.empty())
    {
        ResourceAllocation & allocation = *running_allocations.begin();
        running_allocations.erase(running_allocations.iterator_to(allocation));
        if (allocation.increasing_hook.is_linked())
            increasing_allocations.erase(increasing_allocations.iterator_to(allocation));
        if (allocation.decreasing_hook.is_linked())
            decreasing_allocations.erase(decreasing_allocations.iterator_to(allocation));
        if (allocation.removing_hook.is_linked())
            removing_allocations.erase(removing_allocations.iterator_to(allocation));
        allocation.allocated = 0;
        allocation.allocationFailed(reason);
    }

    chassert(pending_allocations.empty());
    chassert(running_allocations.empty());
    chassert(increasing_allocations.empty());
    chassert(decreasing_allocations.empty());
    chassert(removing_allocations.empty());

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

void AllocationQueue::updateMinMaxAllocated(ResourceCost new_value)
{
    std::lock_guard lock(mutex);
    min_max_allocated = new_value;

    // Reject pending allocations that can never succeed because they exceed the new limit.
    // Unlink the allocation from every intrusive container before notifying the owner — see
    // `purgeQueue` for the full rationale (allocationFailed can synchronously wake an owner
    // thread that destroys the ResourceAllocation, and a still-linked removing_hook would
    // either chassert or cause a use-after-free when `processActivation` later runs).
    for (auto it = pending_allocations.begin(); it != pending_allocations.end();)
    {
        ResourceAllocation & allocation = *it;
        ++it; // Advance before erasing
        if (allocation.increase.size > min_max_allocated)
        {
            pending_allocations.erase(pending_allocations.iterator_to(allocation));
            pending_allocations_size -= allocation.increase.size;
            if (allocation.removing_hook.is_linked())
                removing_allocations.erase(removing_allocations.iterator_to(allocation));
            ++rejects;
            allocation.allocationFailed(std::make_exception_ptr(
                Exception(ErrorCodes::RESOURCE_LIMIT_EXCEEDED,
                    "Workload '{}' allocation of size {} exceeds the limit of {}",
                    getWorkloadName(), formatReadableCost(allocation.increase.size), formatReadableCost(min_max_allocated))));
        }
    }

    // Update increase pointer in case the removed allocation was the current one
    if (setIncrease() && parent)
        propagate(Update().setIncrease(increase));
}

void AllocationQueue::approveIncrease()
{
    std::lock_guard lock(mutex);
    chassert(increase);
    ResourceAllocation & allocation = increase->allocation;
    SCHED_DBG("{} -- approveIncrease(id={}, size={}, allocated={})", getPath(), allocation.id, increase->size, allocated);
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
    // `apply` above incremented `allocations` for `Kind::Pending`/`Kind::Initial`. Mark the
    // allocation as admitted so its eventual removal propagates a matching `removing_allocation`
    // decrease (instead of underflowing `allocations` in the hierarchy).
    if (allocation.increase.kind == IncreaseRequest::Kind::Pending
        || allocation.increase.kind == IncreaseRequest::Kind::Initial)
        allocation.admitted = true;

    // Notify allocation
    increase->allocation.increaseApproved(*increase);
    increase = nullptr;

    setIncrease();
}

void AllocationQueue::approveDecrease()
{
    std::lock_guard lock(mutex);

    chassert(decrease);
    ResourceAllocation & allocation = decrease->allocation;
    SCHED_DBG("{} -- approveDecrease(id={}, size={}, allocated={})", getPath(), allocation.id, decrease->size, allocated);
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

    // Reinsert into the appropriate data structures unless this is a removal
    if (!decrease->removing_allocation)
    {
        running_allocations.insert(allocation);
        if (is_increasing)
            increasing_allocations.insert(allocation);
    }

    // Ordering of increasing allocations is changed - update the next increase request if needed and propagate the update
    if (is_increasing && setIncrease())
        propagate(Update().setIncrease(increase));

    // Notify allocation
    decrease->allocation.decreaseApproved(*decrease);
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
                formatReadableCost(victim.allocated), getWorkloadName(), formatReadableCost(killer.size));
        else
            details = fmt::format("Evicting the largest allocation of size {} in workload '{}' to satisfy increase of a smaller allocation.",
                formatReadableCost(victim.allocated), getWorkloadName());
    }

    return &victim;
}

void AllocationQueue::processActivation()
{
    if (!parent)
        return; // Detached queue - nothing to do
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
            else // Running allocation - cancel pending increase (if any) and prepare decrease to zero
            {
                // Cancel pending increase (safe: we are on the scheduler thread)
                if (allocation.increasing_hook.is_linked())
                {
                    increasing_allocations.erase(increasing_allocations.iterator_to(allocation));
                    running_allocations.erase(running_allocations.iterator_to(allocation));
                    allocation.fair_key = allocation.allocated;
                    running_allocations.insert(allocation);
                }

                // Never-admitted allocation (inserted with `initial_size == 0` and either never
                // grew or had its first `Initial` increase cancelled above). The hierarchy's
                // `allocations` counter was never incremented for it, so propagating a removing
                // decrease would underflow `allocations` in this queue and every ancestor.
                // Remove locally and notify the owner directly.
                if (!allocation.admitted)
                {
                    chassert(allocation.allocated == 0);
                    running_allocations.erase(running_allocations.iterator_to(allocation));
                    allocation.decrease.prepare(0, /*removing_allocation=*/ true);
                    allocation.decreaseApproved(allocation.decrease);
                    continue;
                }

                // Prepare decrease for the full current amount (accurate because increase is cancelled above,
                // or was already approved by the scheduler before this processActivation — either way
                // allocation.allocated reflects the true state).
                // If there is already a pending decrease, update it in-place: parent's pointer chain
                // references the same allocation.decrease object and reads values at approveDecrease time.
                allocation.decrease.prepare(allocation.allocated, /*removing_allocation=*/ true);
                if (!allocation.decreasing_hook.is_linked())
                    decreasing_allocations.push_back(allocation);
            }
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

    // See `updateMinMaxAllocated` for the rationale on unlinking `removing_hook` before
    // calling `allocationFailed`.
    while (max_queued >= 0 && static_cast<size_t>(max_queued) < pending_allocations.size())
    {
        ResourceAllocation & allocation = pending_allocations.back();
        pending_allocations.erase(pending_allocations.iterator_to(allocation));
        pending_allocations_size -= allocation.increase.size;
        if (allocation.removing_hook.is_linked())
            removing_allocations.erase(removing_allocations.iterator_to(allocation));
        allocation.allocationFailed(std::make_exception_ptr(
            Exception(ErrorCodes::SERVER_OVERLOADED,
                "Workload '{}' limit `max_waiting_queries` has been reached: {} of {}",
                getWorkloadName(), pending_allocations.size(), max_queued)));
        ++rejects;
    }

    // Update increase pointer in case the removed allocation was the current one
    if (setIncrease() && parent)
        propagate(Update().setIncrease(increase));
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
