#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/IWorkloadNode.h>
#include <Common/Scheduler/Debug.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <fmt/format.h>

#include <algorithm>

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
        /// Every arrival is a scheduler event. Besides making late fitting work visible, this
        /// avoids reading scheduler-thread-only suspension flags from a query thread.
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
    /// A later fitting increase can become the first eligible request while an older one is parked.
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

bool AllocationQueue::trySuspendIncrease(ResourceAllocation & allocation)
{
    chassert(&allocation.queue == this);

    /// Suction is terminal. Once selected, the request must run or drive eviction; it cannot
    /// return to the spilling queue.
    if (allocation.memory_growth_suction_priority)
        return false;

    /// Every request gets one fit check per resource-state round. The first regular request parked
    /// in a queue is its growth owner; later regular, initial, and pending requests may also yield so
    /// policy nodes can continue searching the constrained subtree.
    if (allocation.memory_growth_suspension_attempted)
    {
        /// A regular request remains in the same eviction-queue entry while its one spill pass is
        /// active or waiting to acquire suction. Re-hide it without opening another spill epoch.
        if (allocation.increase.kind == IncreaseRequest::Kind::Regular
            && allocation.isProtectedFromEviction()
            && allocation.memory_growth_eviction_order != 0)
        {
            allocation.memory_growth_suspended = true;
            memory_growth_suspension_changed = true;
            scheduleActivation();
            return true;
        }
        return false;
    }

    allocation.memory_growth_suspension_attempted = true;
    if (allocation.increase.kind == IncreaseRequest::Kind::Regular
        && allocation.isProtectedFromEviction())
    {
        /// The query setting decides whether entering the eviction queue starts a forced spill.
        /// The suction ceiling may end that spill early after reconciliation, but it must not bypass
        /// an explicitly forced spill before the query-side controller gets the request.
        const auto pressure_action = allocation.onGrowthPressure();
        if (pressure_action == ResourceAllocation::GrowthPressureAction::Protect)
            allocation.memory_growth_recovery_pending = true;
        else if (allocation.canStartSuctionBeforeSpillCompletes())
        {
            /// The spill request remains controlled exclusively by the query setting. This
            /// independent allocation threshold only decides whether suction must wait for it.
            allocation.memory_growth_recovery_pending = true;
            allocation.onGrowthPressureResolved();
        }
        allocation.memory_growth_eviction_order = ++last_eviction_order;
        if (!suspended_growth)
            suspended_growth = &allocation;
    }

    allocation.memory_growth_suspended = true;
    memory_growth_suspension_changed = true;
    /// Recompute the eligible increase on the scheduler thread. Do not take `mutex` here: the parent
    /// `AllocationLimit` may make this decision while an `AllocationQueue` decrease is being propagated.
    scheduleActivation();
    return true;
}

void AllocationQueue::notifyRecoveryProgress(ResourceAllocation & allocation)
{
    std::lock_guard lock(mutex);
    if (is_not_usable)
        return;

    /// Publish against the allocation, not the queue's owner pointer. Completion may arrive before
    /// the scheduler finishes parking the owner; the next activation consumes this durable bit.
    allocation.memory_growth_recovery_pending = true;
    memory_growth_suspension_changed = true;
    scheduleActivation();
}

bool AllocationQueue::retrySuction(ResourceAllocation & allocation)
{
    /// May be called while this queue is propagating a decrease with `mutex` held. All touched
    /// fields are scheduler-thread state, matching `trySuspendIncrease`.
    if (is_not_usable || !allocation.increasing_hook.is_linked())
        return false;
    chassert(allocation.memory_growth_recovery_pending || allocation.memory_growth_suction_priority);
    if (allocation.memory_growth_recovery_pending)
    {
        if (!canEnterSuction(allocation))
            return false;
        allocation.memory_growth_suction_priority = true;
        suction_growth = &allocation;
        allocation.memory_growth_eviction_order = 0;
        if (suspended_growth == &allocation)
            suspended_growth = nullptr;
    }
    allocation.memory_growth_suspended = false;
    memory_growth_suspension_changed = true;
    scheduleActivation();
    return true;
}

bool AllocationQueue::canEnterSuction(const ResourceAllocation & allocation) const
{
    for (ISchedulerNode * node = parent; node; node = node->parent)
    {
        const auto * space_node = static_cast<const ISpaceSharedNode *>(node);
        /// Policy and workload ancestors share the descendant's slot even without a local limit.
        if (ResourceAllocation * suction = space_node->getSuctionAllocation(); suction && suction != &allocation)
            return false;
        /// The queue policy may replace this queue's provisional spiller before suction starts.
        /// A spiller from another queue still owns this ancestor scope and blocks promotion.
        if (ResourceAllocation * spilling = space_node->getLocalSpillingAllocation();
            spilling && spilling != &allocation && &spilling->queue != &allocation.queue)
            return false;
    }
    return true;
}

void AllocationQueue::consumeSuctionClaim(ResourceAllocation & recovering)
{
    chassert(recovering.increasing_hook.is_linked());
    chassert(recovering.memory_growth_recovery_pending);
    chassert(recovering.memory_growth_suction_priority);

    suction_growth = &recovering;
    recovering.memory_growth_recovery_pending = false;
    recovering.memory_growth_eviction_order = 0;
    recovering.memory_growth_suspended = false;
    if (suspended_growth == &recovering)
        suspended_growth = nullptr;

    const ResourceCost old_size = recovering.increase.size;
    const ResourceCost reconciled_size = recovering.reconcilePendingIncrease(recovering.allocated, old_size);
    if (reconciled_size != old_size)
    {
        increasing_allocations.erase(increasing_allocations.iterator_to(recovering));
        running_allocations.erase(running_allocations.iterator_to(recovering));
        recovering.increase.size = reconciled_size;
        recovering.fair_key = recovering.allocated + reconciled_size;
        running_allocations.insert(recovering);
        if (reconciled_size > 0)
            increasing_allocations.insert(recovering);
        else
        {
            recovering.memory_growth_suction_priority = false;
            suction_growth = nullptr;
            recovering.onGrowthPressureResolved();
            recovering.increaseCancelled();

            /// This owner no longer needs memory. Re-open every request hidden while it owned the
            /// search round so available capacity is reconsidered in this activation.
            for (ResourceAllocation & pending : pending_allocations)
            {
                pending.memory_growth_suspended = false;
                pending.memory_growth_suspension_attempted = false;
            }
            for (ResourceAllocation & increasing : increasing_allocations)
                increasing.memory_growth_suspended = false;
        }
    }
    memory_growth_suspension_changed = true;
}

bool AllocationQueue::tryPromoteEvictionQueueHead(IncreaseRequest * & preferred_suction)
{
    chassert(!increase);

    const auto first_nominated = std::find_if(
        increasing_allocations.begin(), increasing_allocations.end(), [](const ResourceAllocation & allocation)
        {
            return allocation.memory_growth_eviction_order != 0;
        });
    if (first_nominated == increasing_allocations.end())
    {
        suspended_growth = nullptr;
        return false;
    }

    const auto queue_policy = first_nominated->getSuctionQueuePolicy();
    auto has_higher_priority = [queue_policy](const ResourceAllocation & lhs, const ResourceAllocation & rhs)
    {
        if (queue_policy == ResourceAllocation::SuctionQueuePolicy::LargestMemoryFirst
            && lhs.allocated != rhs.allocated)
            return lhs.allocated > rhs.allocated;
        return lhs.memory_growth_eviction_order < rhs.memory_growth_eviction_order;
    };

    ResourceAllocation * next = nullptr;
    for (ResourceAllocation & candidate : increasing_allocations)
    {
        const bool waiting_for_suction = candidate.memory_growth_eviction_order != 0
            && candidate.memory_growth_recovery_pending
            && !candidate.memory_growth_suspended;
        if (waiting_for_suction && (!next || has_higher_priority(candidate, *next)))
            next = &candidate;
    }
    if (!next)
        return false;

    suspended_growth = next;
    ResourceAllocation & recovering = *suspended_growth;
    if (!canEnterSuction(recovering))
        return false;

    recovering.memory_growth_suction_priority = true;
    suction_growth = &recovering;
    consumeSuctionClaim(recovering);
    if (recovering.increasing_hook.is_linked() && recovering.memory_growth_suction_priority)
        preferred_suction = &recovering.increase;
    return true;
}

void AllocationQueue::retrySuspendedIncreases()
{
    /// The owning limit may be above policy nodes and sibling queues. Defer the O(n) reset to this
    /// queue's activation so every intrusive container is examined while holding `mutex`.
    memory_growth_suspension_retry_requested = true;
    memory_growth_suspension_changed = true;
    scheduleActivation();
}

bool AllocationQueue::hasSuspendedIncrease() const
{
    return suspended_growth != nullptr;
}

ResourceAllocation * AllocationQueue::getSuctionAllocation() const
{
    return suction_growth && suction_growth->memory_growth_suction_priority ? suction_growth : nullptr;
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
    suspended_growth = nullptr;
    suction_growth = nullptr;
    memory_growth_suspension_retry_requested = false;

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
    active_allocations = 0;
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
    chassert(increase->approval_epoch > 0);
    apply(*increase);
    allocation.allocated += increase->size;
    if (allocation.increase.kind == IncreaseRequest::Kind::Regular)
    {
        allocation.memory_growth_eviction_order = 0;
        allocation.memory_growth_suction_priority = false;
        if (suction_growth == &allocation)
            suction_growth = nullptr;
        allocation.onGrowthPressureResolved();
    }

    if (suspended_growth == &allocation)
    {
        /// A beneficiary released enough memory for the parked growth to fit. End this suspension
        /// round; the remaining allocations continue under the normal queue policy.
        clearMemoryGrowthSuspension();
    }
    /// A successfully approved request gets a fresh suspension chance on its next growth conflict.
    allocation.memory_growth_suspended = false;
    allocation.memory_growth_suspension_attempted = false;
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
    if (decrease->removing_allocation && suction_growth == &allocation)
        suction_growth = nullptr;
    // Reinsert into the appropriate data structures unless this is a removal
    if (!decrease->removing_allocation)
    {
        running_allocations.insert(allocation);
        if (is_increasing)
            increasing_allocations.insert(allocation);
    }

    /// Any released memory is a progress event. Give parked growth another chance immediately, even while
    /// beneficiaries keep running. If it still does not fit, `trySuspendIncrease` parks it again.
    bool ancestor_has_suction = false;
    for (ISchedulerNode * node = parent; node; node = node->parent)
    {
        if (static_cast<ISpaceSharedNode *>(node)->getLocalSuctionAllocation())
        {
            ancestor_has_suction = true;
            break;
        }
    }
    bool retry_suspended_growth = suspended_growth != nullptr && !ancestor_has_suction;
    if (retry_suspended_growth)
    {
        suspended_growth->memory_growth_suspended = false;
        /// Capacity changed, so every alternative rejected in the previous round deserves a fresh
        /// fit check as well. This preserves queue order without letting one oversized request hide
        /// a later fitting request permanently.
        for (ResourceAllocation & pending : pending_allocations)
        {
            pending.memory_growth_suspended = false;
            pending.memory_growth_suspension_attempted = false;
        }
        for (ResourceAllocation & increasing : increasing_allocations)
        {
            increasing.memory_growth_suspended = false;
        }
        memory_growth_suspension_changed = true;
    }

    // Ordering of increasing allocations is changed - update the next increase request if needed and propagate the update
    if (is_increasing || retry_suspended_growth)
    {
        Update update;
        IncreaseRequest * preferred_suction = nullptr;
        bool increase_changed = setIncrease();
        const bool has_active_suction = ancestor_has_suction
            || (increase
                && increase->allocation.memory_growth_suction_priority
                && !increase->allocation.memory_growth_recovery_pending);
        const bool suction_changed = !increase
            && !has_active_suction
            && tryPromoteEvictionQueueHead(preferred_suction);
        if (suction_changed)
            increase_changed = setIncrease(preferred_suction) || increase_changed;

        if (increase_changed || memory_growth_suspension_changed)
            update.setIncrease(increase);
        if (suction_changed)
            update.setSuction(getSuctionAllocation());
        memory_growth_suspension_changed = false;
        if (update)
            propagate(std::move(update));
    }

    // Notify allocation
    decrease->allocation.decreaseApproved(*decrease);
    decrease = nullptr;

    setDecrease();
}

ResourceAllocation * AllocationQueue::selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details)
{
    UNUSED(limit);

    std::lock_guard lock(mutex);

    // A pending allocation must not evict a running allocation in its own queue.
    if (killer.kind == IncreaseRequest::Kind::Pending && &killer.allocation.queue == this)
        return nullptr;

    if (running_allocations.empty())
        return nullptr;

    ResourceAllocation * victim = nullptr;
    ResourceAllocation * protected_victim = nullptr;
    ResourceAllocation * self_fallback = nullptr;
    ResourceAllocation * protected_self_fallback = nullptr;
    /// Search the whole queue in reverse acquisition order. The suctioned requester is the final
    /// fallback, so eviction frees competing allocations one at a time before killing the requester.
    for (auto it = running_allocations.rbegin(); it != running_allocations.rend(); ++it)
    {
        ResourceAllocation & candidate = *it;
        if (candidate.kill_requested)
            continue;
        if (&candidate == &killer.allocation && killer.allocation.isSuctioned())
        {
            if (candidate.isProtectedFromEviction())
                protected_self_fallback = &candidate;
            else
                self_fallback = &candidate;
            continue;
        }
        if (candidate.isProtectedFromEviction())
        {
            if (!protected_victim)
                protected_victim = &candidate;
            continue;
        }
        victim = &candidate;
        break;
    }

    if (!victim)
        victim = self_fallback;
    if (!victim)
        victim = protected_victim;
    if (!victim)
        victim = protected_self_fallback;
    if (!victim)
        return nullptr;

    // If this is the least common ancestor of killer and victim - add details
    if (&killer.allocation.queue == this)
    {
        if (&killer.allocation == victim)
            details = fmt::format("Evicting the largest allocation of size {} in workload '{}' to satisfy its own increase for {}.",
                formatReadableCost(victim->allocated), getWorkloadName(), formatReadableCost(killer.size));
        else
            details = fmt::format("Evicting the largest allocation of size {} in workload '{}' to satisfy increase of a smaller allocation.",
                formatReadableCost(victim->allocated), getWorkloadName());
    }

    return victim;
}

void AllocationQueue::processActivation()
{
    if (!parent)
        return; // Detached queue - nothing to do
    Update update;
    bool suction_changed = false;
    IncreaseRequest * preferred_suction = nullptr;
    {
        std::lock_guard lock(mutex);

        if (memory_growth_suspension_retry_requested)
        {
            /// Forced spill can change actual demand while the original request remains parked.
            /// Reconcile it on the scheduler thread before the next fit check, so retry/eviction
            /// never uses the stale pre-spill size.
            if (suspended_growth && suspended_growth->increasing_hook.is_linked())
            {
                ResourceAllocation & recovering = *suspended_growth;
                const ResourceCost old_size = recovering.increase.size;
                const ResourceCost reconciled_size = recovering.reconcilePendingIncrease(recovering.allocated, old_size);
                if (reconciled_size != old_size)
                {
                    increasing_allocations.erase(increasing_allocations.iterator_to(recovering));
                    running_allocations.erase(running_allocations.iterator_to(recovering));
                    recovering.increase.size = reconciled_size;
                    recovering.fair_key = recovering.allocated + reconciled_size;
                    running_allocations.insert(recovering);

                    if (reconciled_size > 0)
                        increasing_allocations.insert(recovering);
                    else
                    {
                        clearMemoryGrowthSuspension();
                        recovering.increaseCancelled();
                    }
                    memory_growth_suspension_changed = true;
                }

                /// Stop waiting for the rest of the spill pass once the already allocated memory
                /// permits suction to start. The prospective total is checked later, in suction.
                if (recovering.increasing_hook.is_linked()
                    && recovering.isGrowthRecoveryActive()
                    && recovering.canStartSuctionBeforeSpillCompletes())
                {
                    recovering.memory_growth_recovery_pending = true;
                    recovering.onGrowthPressureResolved();
                    memory_growth_suspension_changed = true;
                }
            }

            /// A release elsewhere in the constrained subtree starts a new fit-check round here.
            for (ResourceAllocation & pending : pending_allocations)
            {
                pending.memory_growth_suspended = false;
                pending.memory_growth_suspension_attempted = false;
            }
            for (ResourceAllocation & increasing : increasing_allocations)
                increasing.memory_growth_suspended = false;
            memory_growth_suspension_retry_requested = false;
        }

        // Remove allocation if necessary
        while (!removing_allocations.empty())
        {
            ResourceAllocation & allocation = removing_allocations.front();
            removing_allocations.pop_front(); // Unlink before calling allocationFailed() to avoid use-after-free race
            if (&allocation == suspended_growth)
                clearMemoryGrowthSuspension();
            else
                allocation.memory_growth_eviction_order = 0;
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

        /// A limit claims suction without taking this queue mutex. Consume that claim here, where
        /// the parked request can be reconciled and re-keyed atomically before it becomes visible.
        auto claimed_suction = std::find_if(
            increasing_allocations.begin(), increasing_allocations.end(), [](const ResourceAllocation & allocation)
            {
                return allocation.memory_growth_suction_priority && allocation.memory_growth_recovery_pending;
            });
        if (claimed_suction != increasing_allocations.end())
        {
            consumeSuctionClaim(*claimed_suction);
            if (claimed_suction->increasing_hook.is_linked() && claimed_suction->memory_growth_suction_priority)
                preferred_suction = &claimed_suction->increase;
            suction_changed = true;
        }

        const bool has_active_suction = preferred_suction
            || (increase
                && increase->allocation.increasing_hook.is_linked()
                && increase->allocation.memory_growth_suction_priority
                && !increase->allocation.memory_growth_recovery_pending);

        // Update requests. A completed spill enters suction only after this queue has exhausted
        // every currently visible fitting opportunity. This preserves normal scheduling during
        // the spill phase and makes suction the final transition before eviction.
        bool increase_changed = setIncrease(preferred_suction);
        if (!increase)
        {
            /// A completed spill remains in eviction order until every ancestor scope can give it
            /// the single suction slot. This prevents sibling spillers from competing for the same
            /// release.
            IncreaseRequest * selected_suction = nullptr;
            if (!has_active_suction && tryPromoteEvictionQueueHead(selected_suction))
            {
                suction_changed = true;
                preferred_suction = selected_suction;
                increase_changed = setIncrease(preferred_suction) || increase_changed;
            }
        }

        if (increase_changed || memory_growth_suspension_changed)
        {
            update.setIncrease(increase);
            memory_growth_suspension_changed = false;
        }
        if (setDecrease())
            update.setDecrease(decrease);
        if (suction_changed)
            update.setSuction(getSuctionAllocation());
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

bool AllocationQueue::setIncrease(IncreaseRequest * preferred_suction) // TSA_REQUIRES(mutex)
{
    IncreaseRequest * old_increase = increase;
    auto is_eligible_suction = [](IncreaseRequest * request)
    {
        if (!request)
            return false;
        const ResourceAllocation & allocation = request->allocation;
        return allocation.increasing_hook.is_linked()
            && allocation.memory_growth_suction_priority
            && !allocation.memory_growth_suspended
            && !allocation.memory_growth_recovery_pending;
    };
    IncreaseRequest * suction = is_eligible_suction(preferred_suction)
        ? preferred_suction
        : (is_eligible_suction(old_increase) ? old_increase : nullptr);
    auto eligible = std::find_if(increasing_allocations.begin(), increasing_allocations.end(), [](const ResourceAllocation & allocation)
    {
        return !allocation.memory_growth_suspended && !allocation.memory_growth_recovery_pending;
    });
    auto pending = std::find_if(pending_allocations.begin(), pending_allocations.end(), [](const ResourceAllocation & allocation)
    {
        return !allocation.memory_growth_suspended;
    });

    /// Preserve the scheduler's original order until a growth request is actually suspended.
    if (suction)
        increase = suction;
    else if (eligible != increasing_allocations.end())
        increase = &eligible->increase;
    else if (pending != pending_allocations.end())
        increase = &pending->increase;
    else
        increase = nullptr;

    return increase != old_increase;
}

void AllocationQueue::clearMemoryGrowthSuspension() // TSA_REQUIRES(mutex)
{
    if (suspended_growth)
    {
        suspended_growth->onGrowthPressureResolved();
        suspended_growth->memory_growth_suspended = false;
        suspended_growth->memory_growth_eviction_order = 0;
        suspended_growth->memory_growth_recovery_pending = false;
        suspended_growth->memory_growth_suction_priority = false;
        if (suction_growth == suspended_growth)
            suction_growth = nullptr;
    }
    suspended_growth = nullptr;
    memory_growth_suspension_retry_requested = false;
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
