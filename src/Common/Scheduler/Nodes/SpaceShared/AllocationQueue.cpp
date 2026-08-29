#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/IWorkloadNode.h>
#include <Common/Scheduler/Debug.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <fmt/format.h>

#include <algorithm>
#include <utility>

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
    refreshUnusedCapacityReclaimPending();
    if (&allocation == &*decreasing_allocations.begin())
        scheduleActivation();
}

bool AllocationQueue::trySuspendIncrease(ResourceAllocation & allocation)
{
    chassert(&allocation.queue == this);

    /// Every request gets one fit check per resource-state round. The first regular request parked
    /// in a queue is its growth owner; later regular, initial, and pending requests may also yield so
    /// policy nodes can continue searching the constrained subtree.
    if (allocation.memory_growth_suspension_attempted)
        return false;

    allocation.memory_growth_suspension_attempted = true;
    if (allocation.increase.kind == IncreaseRequest::Kind::Regular)
    {
        /// Once admitted work blocks on its own growth it is no longer a productive beneficiary.
        /// Notify every stacked limit before recording the ending epoch here, mirroring the way
        /// approval epochs are recorded only after all limits observe a new approval.
        if (allocation.last_increase_approval_epoch > allocation.last_productivity_end_epoch)
        {
            endProductiveMembership(allocation);
            allocation.last_productivity_end_epoch = allocation.last_increase_approval_epoch;
        }

        /// Protection is externally injected suction priority, not permission to bypass fitting
        /// work. Remember it, then hide the blocked growth for one complete policy-scoped search.
        /// AllocationLimit consumes the decision only after every currently visible alternative
        /// has been considered.
        /// Keep one queue-level policy owner, but do not conflate that with query-side recovery.
        /// Every blocked allocation must enter its own recovery lane before the policy owner reaches
        /// the shared last-resort path. Allocations from one dependency graph may still coalesce that
        /// work through their shared query-scoped spill controller.
        if (!suspended_growth)
            suspended_growth = &allocation;
        if (allocation.onGrowthPressure() == ResourceAllocation::GrowthPressureAction::Protect)
            allocation.memory_growth_suction_priority = true;
    }

    allocation.memory_growth_suspended = true;
    memory_growth_suspension_changed = true;
    /// Recompute the eligible increase on the scheduler thread. Do not take `mutex` here: the parent
    /// `AllocationLimit` may make this decision while an `AllocationQueue` decrease is being propagated.
    scheduleActivation();
    return true;
}

void AllocationQueue::notifyRecoveryProgress(ResourceAllocation & allocation, UInt64 recovery_epoch)
{
    std::lock_guard lock(mutex);
    if (is_not_usable || !allocation.increasing_hook.is_linked()
        || allocation.increase.kind != IncreaseRequest::Kind::Regular
        || !allocation.acceptRecoveryProgress(recovery_epoch))
        return;

    /// Validation and publication are one queue-serialized hand-off. Completion may arrive before
    /// the scheduler finishes parking the owner, but it cannot attach to a later reuse of the
    /// allocation's embedded IncreaseRequest.
    allocation.memory_growth_recovery_pending = true;
    scheduleActivation();
}

void AllocationQueue::notifyUnusedCapacity(ResourceAllocation & allocation)
{
    std::lock_guard lock(mutex);
    if (is_not_usable || (!allocation.running_hook.is_linked() && !allocation.increasing_hook.is_linked()))
        return;
    unused_capacity_changed = true;
    scheduleActivation();
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
    refreshUnusedCapacityReclaimPending();
    if (&allocation == &*removing_allocations.begin())
        scheduleActivation();
}

void AllocationQueue::purgeQueue()
{
    std::lock_guard lock(mutex);
    chassert(parent == nullptr);
    cancelActivation();
    suspended_growth = nullptr;
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
    unused_capacity_reclaim_pending.store(false, std::memory_order_release);

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
    allocation.last_increase_approval_epoch = increase->approval_epoch;
    if (allocation.increase.kind == IncreaseRequest::Kind::Regular)
    {
        /// Approval can beat an already-queued recovery-completion activation. Consume the exact
        /// allocation's durable bit here so it cannot be mistaken for completion of a later grow.
        allocation.memory_growth_recovery_pending = false;
        if (suspended_growth != &allocation)
        {
            allocation.memory_growth_suction_priority = false;
            allocation.onGrowthPressureResolved();
        }
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
    if (allocation.takeUnusedCapacityNotification())
    {
        unused_capacity_changed = true;
        scheduleActivation();
    }
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
    refreshUnusedCapacityReclaimPending();

    // We need to remove from running/increasing allocations to update the key
    running_allocations.erase(running_allocations.iterator_to(allocation));
    bool is_increasing = allocation.increasing_hook.is_linked();
    if (is_increasing)
        increasing_allocations.erase(increasing_allocations.iterator_to(allocation));

    // Update the key and other fields
    apply(*decrease);
    allocation.allocated -= decrease->size;
    allocation.fair_key -= decrease->size;
    if (allocation.allocated == 0)
        allocation.last_productivity_end_epoch = allocation.last_increase_approval_epoch;

    // Reinsert into the appropriate data structures unless this is a removal
    if (!decrease->removing_allocation)
    {
        running_allocations.insert(allocation);
        if (is_increasing)
            increasing_allocations.insert(allocation);
    }

    /// Any released memory is a progress event. Give parked growth another chance immediately, even while
    /// beneficiaries keep running. If it still does not fit, `trySuspendIncrease` parks it again.
    bool retry_suspended_growth = suspended_growth != nullptr;
    if (retry_suspended_growth)
    {
        suspended_growth->memory_growth_suspended = false;
        suspended_growth->memory_growth_suspension_attempted = false;
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
            increasing.memory_growth_suspension_attempted = false;
        }
        memory_growth_suspension_changed = true;
    }

    // Ordering of increasing allocations is changed - update the next increase request if needed and propagate the update
    if ((is_increasing || retry_suspended_growth) && (setIncrease() || memory_growth_suspension_changed))
    {
        memory_growth_suspension_changed = false;
        propagate(Update().setIncrease(increase));
    }

    // Notify allocation
    decrease->allocation.decreaseApproved(*decrease);
    decrease = nullptr;

    setDecrease();
}

UnusedCapacityReclaimResult AllocationQueue::reclaimUnusedCapacity(IncreaseRequest &, ResourceCost max_size, bool)
{
    if (max_size <= 0)
        return {};

    DecreaseRequest * reclaimed = nullptr;
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            return {};

        /// A query-thread decrease may already be committed but not yet published by this leaf's
        /// activation. Treat it as progress instead of claiming a second donor or declaring the
        /// local round exhausted. Its ordinary decrease approval is the durable wakeup.
        auto existing_release = std::find_if(decreasing_allocations.begin(), decreasing_allocations.end(), [](const ResourceAllocation & allocation)
        {
            return allocation.decrease.size > 0;
        });
        if (existing_release != decreasing_allocations.end())
            reclaimed = &existing_release->decrease;

        /// Release from the largest allocations first, mirroring victim order while avoiding a
        /// kill entirely. The allocation callback only commits currently unused capacity; the
        /// ordinary decrease path performs all accounting and hierarchy propagation.
        for (auto it = running_allocations.rbegin(); it != running_allocations.rend() && !reclaimed; ++it)
        {
            ResourceAllocation & allocation = *it;
            if (allocation.decreasing_hook.is_linked() || allocation.removing_hook.is_linked())
                continue;

            const ResourceCost amount = allocation.reclaimUnusedCapacity(max_size);
            chassert(amount >= 0 && amount <= max_size);
            if (amount == 0)
                continue;

            allocation.decrease.prepare(amount, /*removing_allocation=*/ false);
            decreasing_allocations.push_back(allocation);
            refreshUnusedCapacityReclaimPending();
            reclaimed = &allocation.decrease;
            break; // Publish and acknowledge one ordinary decrease before claiming another.
        }
    }

    /// Do not propagate synchronously while an ancestor is evaluating an increase. Activation
    /// publishes the decrease on the next scheduler turn, where decreases already have priority.
    if (reclaimed)
        scheduleActivation();
    return {.decrease = reclaimed};
}

bool AllocationQueue::hasUnusedCapacityReclaimPending() const
{
    return unused_capacity_reclaim_pending.load(std::memory_order_acquire);
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

    /// Eviction is graph-wide last resort. Before applying largest-first victim order, prove that
    /// every other blocked dependency graph in this queue has completed its own explicit recovery
    /// pass. This pre-scan is necessary when the killer itself is the largest allocation: the
    /// reverse-order victim loop stops at the requester and would otherwise never see a smaller
    /// graph whose recovery is still active.
    const bool another_graph_is_recovering = std::any_of(
        running_allocations.begin(), running_allocations.end(), [&](ResourceAllocation & candidate)
        {
            return &candidate != &killer.allocation
                && !candidate.kill_requested
                && candidate.increasing_hook.is_linked()
                && candidate.increase.kind == IncreaseRequest::Kind::Regular
                && (candidate.isGrowthRecoveryActive() || !candidate.memory_growth_suction_priority);
        });
    if (another_graph_is_recovering)
        return nullptr;

    ResourceAllocation * victim = nullptr;
    ResourceAllocation * self_fallback = nullptr;
    bool productive_work_is_protected = false;
    const UInt64 protection_epoch = killer.allocation.memory_growth_candidate_protection_epoch;

    /// Search the whole queue. The parked owner is a last fallback, and work approved after the
    /// suspension remains protected while productive. Already-killed candidates cannot pin a
    /// later pressure decision forever.
    for (auto it = running_allocations.rbegin(); it != running_allocations.rend(); ++it)
    {
        ResourceAllocation & candidate = *it;
        if (candidate.kill_requested)
            continue;
        if (&candidate == &killer.allocation)
        {
            self_fallback = &candidate;
            /// running_allocations is already in reverse victim order. Once the requester itself
            /// is reached, every remaining candidate is smaller; killing one of them would invert
            /// the existing largest-first isolation policy. Keep the requester as the fallback and
            /// stop, unless productive work seen above still postpones eviction altogether.
            break;
        }
        if (candidate.increasing_hook.is_linked()
            && candidate.increase.kind == IncreaseRequest::Kind::Regular
            && (candidate.isGrowthRecoveryActive() || !candidate.memory_growth_suction_priority))
        {
            /// One graph's suction decision cannot consume a different graph that has not completed
            /// its own explicit recovery pass. Once that graph publishes completion and injects its
            /// own priority, it re-enters the ordinary victim order.
            productive_work_is_protected = true;
            continue;
        }
        if (protection_epoch != 0
            && candidate.last_increase_approval_epoch > protection_epoch
            && candidate.last_increase_approval_epoch > candidate.last_productivity_end_epoch)
        {
            productive_work_is_protected = true;
            continue;
        }
        victim = &candidate;
        break;
    }

    if (!victim && !productive_work_is_protected)
        victim = self_fallback;
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
    Update update;
    bool notify_unused_capacity = false;
    {
        std::lock_guard lock(mutex);

        /// Recovery completion belongs to the exact allocation/query graph, not to this queue's
        /// singleton policy owner. Consume every published completion before reopening one complete
        /// queue-policy pass.
        bool recovery_completed = false;
        while (true)
        {
            auto recovering_it = std::find_if(increasing_allocations.begin(), increasing_allocations.end(), [](const ResourceAllocation & allocation)
            {
                return allocation.memory_growth_recovery_pending;
            });
            if (recovering_it == increasing_allocations.end())
                break;

            ResourceAllocation & recovering = *recovering_it;
            recovering.memory_growth_recovery_pending = false;
            const ResourceCost old_size = recovering.increase.size;
            const ResourceCost reconciled_size = recovering.reconcilePendingIncrease(recovering.allocated, old_size);
            if (reconciled_size != old_size)
            {
                increasing_allocations.erase(recovering_it);
                running_allocations.erase(running_allocations.iterator_to(recovering));
                recovering.increase.size = reconciled_size;
                recovering.fair_key = recovering.allocated + reconciled_size;
                running_allocations.insert(recovering);

                if (reconciled_size > 0)
                    increasing_allocations.insert(recovering);
                else
                {
                    if (&recovering == suspended_growth)
                        clearMemoryGrowthSuspension();
                    else
                    {
                        recovering.memory_growth_suction_priority = false;
                        recovering.onGrowthPressureResolved();
                    }
                    recovering.increaseCancelled();
                    if (recovering.takeUnusedCapacityNotification())
                        unused_capacity_changed = true;
                }
            }
            recovery_completed = true;
        }

        if (recovery_completed)
        {
            memory_growth_suspension_retry_requested = true;
            memory_growth_suspension_changed = true;
        }

        if (memory_growth_suspension_retry_requested)
        {
            /// A release elsewhere in the constrained subtree starts a new fit-check round here.
            for (ResourceAllocation & pending : pending_allocations)
            {
                pending.memory_growth_suspended = false;
                pending.memory_growth_suspension_attempted = false;
            }
            for (ResourceAllocation & increasing : increasing_allocations)
            {
                increasing.memory_growth_suspended = false;
                increasing.memory_growth_suspension_attempted = false;
            }
            memory_growth_suspension_retry_requested = false;
        }

        // Remove allocation if necessary
        while (!removing_allocations.empty())
        {
            ResourceAllocation & allocation = removing_allocations.front();
            removing_allocations.pop_front(); // Unlink before calling allocationFailed() to avoid use-after-free race
            if (&allocation == suspended_growth)
                clearMemoryGrowthSuspension();
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
                    const bool cancelled_recovery = allocation.increase.kind == IncreaseRequest::Kind::Regular;
                    increasing_allocations.erase(increasing_allocations.iterator_to(allocation));
                    running_allocations.erase(running_allocations.iterator_to(allocation));
                    allocation.fair_key = allocation.allocated;
                    running_allocations.insert(allocation);
                    if (cancelled_recovery)
                    {
                        /// A non-owner reservation can be removed while its own recovery lane is
                        /// active. The singleton-owner cleanup above cannot resolve that episode.
                        allocation.memory_growth_recovery_pending = false;
                        allocation.memory_growth_suction_priority = false;
                        allocation.onGrowthPressureResolved();
                    }
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

        refreshUnusedCapacityReclaimPending();

        // Update requests
        if (setIncrease() || memory_growth_suspension_changed)
        {
            update.setIncrease(increase);
            memory_growth_suspension_changed = false;
        }
        if (setDecrease())
            update.setDecrease(decrease);
        if (parent)
            notify_unused_capacity = std::exchange(unused_capacity_changed, false);
    }

    // Propagate update to parent
    if (parent && update)
        propagate(std::move(update));
    if (notify_unused_capacity && parent)
        castParent().notifyUnusedCapacityAvailable();
}

void AllocationQueue::refreshUnusedCapacityReclaimPending()
{
    bool pending = std::any_of(decreasing_allocations.begin(), decreasing_allocations.end(), [](const ResourceAllocation & allocation)
    {
        return allocation.decrease.size > 0;
    });
    if (!pending)
    {
        pending = std::any_of(removing_allocations.begin(), removing_allocations.end(), [](const ResourceAllocation & allocation)
        {
            return allocation.allocated > 0;
        });
    }
    unused_capacity_reclaim_pending.store(pending, std::memory_order_release);
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
    auto eligible = std::find_if(increasing_allocations.begin(), increasing_allocations.end(), [](const ResourceAllocation & allocation)
    {
        return !allocation.memory_growth_suspended;
    });
    auto pending = std::find_if(pending_allocations.begin(), pending_allocations.end(), [](const ResourceAllocation & allocation)
    {
        return !allocation.memory_growth_suspended;
    });

    /// Do not let request quantization decide who runs. If a fitting admission is already queued
    /// and regular growth would consume the capacity it needs, admit the smaller work first.
    const ResourceCost available = allocated < min_max_allocated ? min_max_allocated - allocated : 0;
    const bool pending_precedes_growth = suspended_growth
        && eligible != increasing_allocations.end()
        && pending != pending_allocations.end()
        && pending->increase.size <= available
        && eligible->increase.size + pending->increase.size > available;

    if (pending_precedes_growth)
        increase = &pending->increase;
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
        suspended_growth->memory_growth_recovery_pending = false;
        suspended_growth->memory_growth_suction_priority = false;
    }
    suspended_growth = nullptr;
    memory_growth_suspension_retry_requested = false;

    /// Beneficiaries are always running allocations. Avoid allocating an auxiliary container here: this
    /// path exists specifically for memory pressure, so an O(n) cleanup is preferable to extra allocation.
    for (ResourceAllocation & allocation : running_allocations)
    {
        allocation.memory_growth_suspended = false;
        allocation.memory_growth_suspension_attempted = false;
    }
    for (ResourceAllocation & allocation : pending_allocations)
    {
        allocation.memory_growth_suspended = false;
        allocation.memory_growth_suspension_attempted = false;
    }
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
