#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_LIMIT_EXCEEDED;
}

AllocationLimit::AllocationLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, ResourceCost max_allocated_)
    : ISpaceSharedNode(event_queue_, info_)
    , max_allocated(max_allocated_)
{}

AllocationLimit::~AllocationLimit()
{
    // We need to clear `parent` in child to avoid dangling references
    if (child)
        removeChild(child.get());
}

void AllocationLimit::updateLimit(UInt64 new_max_allocated)
{
    max_allocated = new_max_allocated;
    // Propagate new effective limit to children
    if (!child)
        return;
    child->updateMinMaxAllocated(std::min(min_max_allocated, max_allocated));
    // WARNING: We do not force eviction here in cases there is no pending increase request to simplify logic.
    // WARNING: Eventually on the first increase request the limit will be applied.
    if (setIncrease(child->increase, true))
        propagate(Update().setIncrease(increase));
}

ResourceCost AllocationLimit::getLimit() const
{
    return max_allocated;
}

std::string_view AllocationLimit::getTypeName() const { return "allocation_limit"; }

void AllocationLimit::attachChild(const std::shared_ptr<ISchedulerNode> & child_)
{
    child = std::static_pointer_cast<ISpaceSharedNode>(child_);
    child->setParentNode(this);
    child->updateMinMaxAllocated(std::min(min_max_allocated, max_allocated));
    propagateUpdate(*child, Update()
        .setAttached(child.get())
        .setIncrease(child->increase)
        .setDecrease(child->decrease)
        .setSuction(child->getSuctionAllocation()));
}

void AllocationLimit::removeChild(ISchedulerNode * child_)
{
    if (child.get() != child_)
        return;
    propagateUpdate(*child, Update()
        .setDetached(child.get())
        .setIncrease(nullptr)
        .setDecrease(nullptr));
    child->setParentNode(nullptr);
    child->updateMinMaxAllocated(std::numeric_limits<ResourceCost>::max());
    child.reset();
}

ISchedulerNode * AllocationLimit::getChild(const String & child_name)
{
    if (child && child->basename == child_name)
        return child.get();
    return nullptr;
}

ResourceAllocation * AllocationLimit::selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details)
{
    if (!child)
        return nullptr;
    return child->selectAllocationToKill(killer, limit, details);
}

void AllocationLimit::approveIncrease()
{
    SCHED_DBG("{} -- approveIncrease({})", getPath(), increase->allocation.id);
    chassert(increase);
    chassert(increase->approval_epoch > 0);
    const bool completes_recovery = increase == recovery_growth;
    const bool completes_final_retry = completes_recovery && increase->allocation.isSuctioned();
    if (completes_recovery)
        clearMemoryGrowthSuspension();
    apply(*increase);
    increase = nullptr;
    child->approveIncrease();
    setIncrease(child->increase, false);
    if (completes_final_retry)
        retrySuctionWaiters();
}

void AllocationLimit::approveDecrease()
{
    SCHED_DBG("{} -- approveDecrease({})", getPath(), decrease->allocation.id);

    chassert(decrease);
    apply(*decrease);

    ResourceAllocation & decreased_allocation = decrease->allocation;
    const bool removed_recovery_owner = recovery_growth
        && &decreased_allocation == &recovery_growth->allocation
        && decrease->removing_allocation;
    const bool recovery_is_final_retry = recovery_growth
        && recovery_growth->allocation.isSuctioned();

    if (&decrease->allocation == allocation_to_kill && decrease->removing_allocation)
        allocation_to_kill = nullptr;
    if (removed_recovery_owner)
    {
        if (recovery_is_final_retry)
            clearSuction();
        else
            clearMemoryGrowthSuspension();
    }

    if (recovery_growth && decrease->size > 0)
    {
        if (recovery_growth->allocation.isSuctioned())
        {
            /// During the final retry, the recovery owner is the only local beneficiary.
            recovery_growth->allocation.queue.retrySuction(recovery_growth->allocation);
        }
        else
        {
            recovery_growth_retry_pending = true;
            child->retrySuspendedIncreases();
        }
    }

    IncreaseRequest * old_increase = increase;

    /// Keep the in-flight decrease visible while the child approves it. The child may propagate an
    /// updated increase before returning, and victim selection can recurse into the same queue while
    /// that queue still holds its mutex. Clear the guard only after child approval.
    child->approveDecrease();
    setDecrease(child->decrease);
    if (!recovery_growth_retry_pending && old_increase == increase && setIncrease(child->increase, true))
        propagate(Update().setIncrease(increase));
}

void AllocationLimit::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    SCHED_DBG("{} -- propagateUpdate(from_child={}, update={})", getPath(), from_child.basename, update.toString());
    chassert(&from_child == child.get());
    bool detached_suspended_subtree = false;
    bool detached_suction_subtree = false;
    if (update.detached && recovery_growth)
    {
        for (ISchedulerNode * node = &recovery_growth->allocation.queue; node; node = node->parent)
        {
            if (node == update.detached)
            {
                detached_suspended_subtree = true;
                break;
            }
        }
    }
    if (update.detached && recovery_growth)
    {
        for (ISchedulerNode * node = &recovery_growth->allocation.queue; node; node = node->parent)
        {
            if (node == update.detached)
            {
                detached_suction_subtree = true;
                break;
            }
        }
    }

    apply(update);

    if (update.suction)
    {
        ResourceAllocation * child_suction = *update.suction;
        if (child_suction)
        {
            IncreaseRequest * child_recovery_request = &child_suction->increase;
            const bool replaces_queue_owner = recovery_growth
                && &recovery_growth->allocation.queue == &child_suction->queue;
            chassert(!recovery_growth || recovery_growth == child_recovery_request || replaces_queue_owner);
            if (!recovery_growth || recovery_growth == child_recovery_request || replaces_queue_owner)
            {
                recovery_growth = child_recovery_request;
                recovery_growth_retry_pending = false;
            }
        }
        else if (recovery_growth
            && recovery_growth->allocation.isSuctioned()
            && !child->getSuctionAllocation())
        {
            clearSuction();
        }
        update.setSuction(getSuctionAllocation());
    }
    bool reapply_constraint = false;
    if (update.attached)
        reapply_constraint = true;
    if (update.detached)
    {
        // The victim referenced by `allocation_to_kill` might be anywhere inside the detached
        // subtree, and `purgeQueue` will fail its owner via `fail_reason` without driving a
        // `removing_allocation=true` decrease back up to clear this pointer through
        // `approveDecrease`. The pointer can therefore outlive the allocation it points at.
        // Comparing against `&allocation_to_kill->queue` would dereference that dangling
        // pointer (heap-use-after-free observed by ASan in
        // `test_cancel_query_with_memory_reservation`).
        //
        // Clear unconditionally on any subtree detach: aggregate counters (`allocated`,
        // `allocations`) are already kept consistent by `apply(Update)` decrementing by the
        // detached subtree's totals, so the only state that can survive incorrectly is this
        // per-victim pointer. If the increase that drove the original kill is still alive, the
        // next `setIncrease(..., reapply_constraint=true)` below picks a fresh victim. The
        // previously-issued `killAllocation` is harmless if its target has already cleaned up.
        allocation_to_kill = nullptr;
        /// A topology update is not a pressure-resolution event. Preserve the episode when an
        /// unrelated sibling detaches; clear it only when its owning subtree is the one removed.
        if (detached_suspended_subtree)
            clearMemoryGrowthSuspension();
        if (detached_suction_subtree)
            clearSuction();
        reapply_constraint = true;
    }
    // Publish the decrease BEFORE evaluating the increase: the eviction decision in `setIncrease` skips
    // victim selection while a release is in flight below, so when a single update carries both a decrease
    // and an increase, the decrease must be visible first.
    if (update.decrease)
    {
        if (setDecrease(*update.decrease))
            update.setDecrease(decrease);
        else
            update.resetDecrease();
    }
    if (update.increase || reapply_constraint)
    {
        if (setIncrease(update.increase ? *update.increase : increase, reapply_constraint))
            update.setIncrease(increase);
        else
            update.resetIncrease();
    }
    if (parent && update)
        propagate(std::move(update));
}

bool AllocationLimit::setIncrease(IncreaseRequest * new_increase, bool reapply_constraint)
{
    if (new_increase && new_increase->allocation.isIncreaseSuspended())
        new_increase = nullptr;

    if (new_increase && new_increase->allocation.memory_growth_suction_priority)
    {
        /// A child suction must be followed by every ancestor on its path. The child queue may
        /// replace its provisional spiller before suction starts, but cannot replace a suction or
        /// a spiller owned by another queue in this scope.
        const bool replaces_queue_owner = recovery_growth
            && &recovery_growth->allocation.queue == &new_increase->allocation.queue;
        chassert(!recovery_growth || recovery_growth == new_increase || replaces_queue_owner);
        if (recovery_growth && recovery_growth != new_increase && !replaces_queue_owner)
            new_increase = nullptr;
        else
        {
            recovery_growth = new_increase;
            recovery_growth_retry_pending = false;
        }
    }

    /// The active suction is the only request allowed to consume a local release. If a policy
    /// temporarily presents another request, keep it queued and reactivate the slot owner.
    if (recovery_growth
        && recovery_growth->allocation.isSuctioned()
        && new_increase != recovery_growth)
    {
        recovery_growth->allocation.queue.retrySuction(recovery_growth->allocation);
        new_increase = nullptr;
    }

    if (!new_increase && !recovery_growth)
    {
        // There is no increase request to satisfy anymore, so forget any victim we were
        // reclaiming from. The killer increase that selected `allocation_to_kill` is gone — its
        // requester finished, was killed, or (for a never-admitted self-kill, e.g. a query with no
        // `reserve_memory` that hits the limit on its first increase) was removed via the local
        // path in `AllocationQueue::processActivation`, which never drives a `removing_allocation`
        // decrease up to `approveDecrease`. Leaving the pointer set would make the next over-limit
        // increase see a non-null `allocation_to_kill`, skip issuing a fresh kill, and block forever
        // (observed as a 600s timeout in `test_scheduler_memory::test_max_memory_limit`). This must
        // run before the early return below, because in the self-kill case both `increase` and
        // `new_increase` are already `nullptr`. Any previously-issued `killAllocation` is harmless
        // if its target has already cleaned up.
        allocation_to_kill = nullptr;
    }

    /// A non-reapply call represents a fresh child-policy observation (normally an activation).
    /// It completes the deferred hierarchy update requested by the previous suspension/retry.
    if (!reapply_constraint && recovery_growth_retry_pending)
        recovery_growth_retry_pending = false;

    /// Spill completion becomes suction only after this constrained scope has no normal request
    /// left to present. The query owns completion; the limit that nominated it owns promotion.
    bool promoted_to_suction = false;
    if (!new_increase
        && recovery_growth
        && !recovery_growth_retry_pending
        && recovery_growth->allocation.memory_growth_recovery_pending
        && decrease == nullptr
        && !allocation_to_kill)
    {
        if (recovery_growth->allocation.queue.retrySuction(recovery_growth->allocation))
            promoted_to_suction = true;
    }

    if (!promoted_to_suction
        && !new_increase
        && recovery_growth
        && recovery_growth->allocation.isSuctioned()
        && decrease == nullptr
        && !allocation_to_kill)
        processSuction();

    /// A suction cap applies even when reserved capacity makes the workload fit check succeed.
    if (new_increase && new_increase->allocation.isSuctioned()
        && !new_increase->allocation.canAllocateInSuction(new_increase->size))
    {
        processSuction();
        const bool changed = increase != nullptr;
        increase = nullptr;
        return changed;
    }

    if (!reapply_constraint && increase == new_increase)
        return false;
    IncreaseRequest * old_increase = increase;
    if (new_increase)
    {
        const ResourceCost effective_limit = getEffectiveLimit(*new_increase);
        if (allocated + new_increase->size > effective_limit)
        {
            // Limit would be violated, so we have to reclaim resource.
            // Do not select a victim while a decrease is pending below: `allocated` still contains
            // memory that is about to be released, so the eviction may be unnecessary. The increase
            // stays blocked, and every decrease approval re-runs this via `reapply_constraint`; once
            // the releases prove insufficient and no decrease is pending, the eviction fires.
            if (!allocation_to_kill && decrease == nullptr)
            {
                if (new_increase == recovery_growth && new_increase->allocation.isSuctioned())
                {
                    processSuction();
                    increase = nullptr;
                    return increase != old_increase;
                }

                /// Preserve the old scheduler boundary: recovery applies only after the existing
                /// victim policy proves that this request would evict an allocation.
                if (!recovery_growth && new_increase->kind == IncreaseRequest::Kind::Regular)
                {
                    String nomination_details;
                    if (!selectAllocationToKill(*new_increase, effective_limit, nomination_details))
                    {
                        increase = nullptr;
                        return increase != old_increase;
                    }
                }

                /// Before evicting a running query for asking for more memory, park that growth once.
                /// The child can then expose other work hidden behind running-query growth. Memory releases
                /// retry the parked growth, so independent work can continue while pressure drains. If
                /// nothing can make progress, the existing kill policy remains the fallback.
                const bool retrying_suspended_owner = new_increase == recovery_growth;
                bool suspended = false;
                if (!recovery_growth)
                {
                    suspended = new_increase->kind == IncreaseRequest::Kind::Regular
                        && new_increase->allocation.canRecoverFromGrowthPressure()
                        && new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                    if (suspended)
                    {
                        recovery_growth = new_increase;
                    }
                }
                else if (new_increase == recovery_growth)
                {
                    suspended = new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                }
                else
                {
                    /// Keep searching across the complete constrained subtree. The request's own
                    /// queue hides it for this round, allowing policy nodes to expose later siblings.
                    suspended = new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                }

                if (suspended)
                {
                    /// A newly parked owner still has to disappear through the child policy before
                    /// exhaustion is meaningful. When an existing owner resurfaces, that traversal
                    /// has already completed; do not depend on a leaf self-activation that may be
                    /// coalesced with the activation currently being processed.
                    recovery_growth_retry_pending = !retrying_suspended_owner;

                    SCHED_DBG("{} -- suspending increase(allocated={}, increase_size={}, max={}, allocation={})",
                        getPath(), allocated, new_increase->size, effective_limit, new_increase->allocation.id);

                }
                else if (!recovery_growth
                    || new_increase->kind != IncreaseRequest::Kind::Regular
                    || !new_increase->allocation.canRecoverFromGrowthPressure())
                {
                    /// A request without recovery enabled only yields once to let the current recovery search
                    /// advance. If it still cannot fit when it resurfaces, retain the old eviction
                    /// behavior instead of making another query's opt-in policy park it indefinitely.
                    selectAndKill(*new_increase);
                }
            }
            // Block until there is enough resource to process child's increase request
            increase = nullptr;
        }
        else
            increase = child->increase; // Can safely process child's increase request
    }
    else
    {
        increase = nullptr; // No more increase requests
    }

    return increase != old_increase;
}

void AllocationLimit::retrySuspendedIncreases()
{
    if (child)
        child->retrySuspendedIncreases();
}

bool AllocationLimit::hasSuspendedIncrease() const
{
    return recovery_growth || (child && child->hasSuspendedIncrease());
}

ResourceAllocation * AllocationLimit::getLocalSpillingAllocation() const
{
    return recovery_growth && !recovery_growth->allocation.isSuctioned()
        ? &recovery_growth->allocation
        : nullptr;
}

ResourceAllocation * AllocationLimit::getLocalSuctionAllocation() const
{
    return recovery_growth && recovery_growth->allocation.isSuctioned()
        ? &recovery_growth->allocation
        : nullptr;
}

ResourceAllocation * AllocationLimit::getSuctionAllocation() const
{
    if (auto * local = getLocalSuctionAllocation())
        return local;
    return child ? child->getSuctionAllocation() : nullptr;
}

void AllocationLimit::clearMemoryGrowthSuspension()
{
    if (recovery_growth)
    {
        recovery_growth->allocation.onGrowthPressureResolved();
        recovery_growth->allocation.memory_growth_suction_priority = false;
    }
    recovery_growth = nullptr;
    recovery_growth_retry_pending = false;
    if (child)
        child->retrySuspendedIncreases();
}

void AllocationLimit::clearSuction()
{
    if (!recovery_growth)
        return;
    recovery_growth->allocation.memory_growth_suction_priority = false;
    recovery_growth->allocation.onGrowthPressureResolved();
    recovery_growth = nullptr;
    recovery_growth_retry_pending = false;
    retrySuctionWaiters();
}

void AllocationLimit::retrySuctionWaiters()
{
    /// A sibling limit may be waiting for the slot at a shared policy ancestor. Retry from the
    /// resource root after releasing ownership so that sibling receives the completion event too.
    ISpaceSharedNode * root = this;
    while (root->parent)
        root = static_cast<ISpaceSharedNode *>(root->parent);
    root->retrySuspendedIncreases();
}

void AllocationLimit::processSuction()
{
    if (!recovery_growth || decrease != nullptr || allocation_to_kill
        || !recovery_growth->allocation.memory_growth_suction_priority)
        return;

    /// Ownership follows a child upward, but eviction still requires pressure at this limit.
    /// A hidden child request may already be waiting for a victim's release farther down.
    if (recovery_growth->allocation.canAllocateInSuction(recovery_growth->size)
        && allocated + recovery_growth->size <= getEffectiveLimit(*recovery_growth))
        return;

    selectAndKill(*recovery_growth);
}

bool AllocationLimit::isTopLevelLimit() const
{
    for (ISchedulerNode * node = parent; node; node = node->parent)
    {
        if (node->getTypeName() == "allocation_limit")
            return false;
    }
    return true;
}

ResourceCost AllocationLimit::getEffectiveLimit(const IncreaseRequest & request) const
{
    if (!isTopLevelLimit() || request.allocation.isSuctioned())
        return max_allocated;

    const UInt64 reserved = request.allocation.getSuctionReservedBytes();
    if (reserved == 0)
        return max_allocated;
    if (reserved >= static_cast<UInt64>(max_allocated))
        return 0;
    return max_allocated - static_cast<ResourceCost>(reserved);
}


void AllocationLimit::selectAndKill(IncreaseRequest & killer)
{
    String details;
    if (killer.allocation.isSuctioned() && !killer.allocation.canAllocateInSuction(killer.size))
    {
        if (killer.allocation.kill_requested)
            return;
        allocation_to_kill = &killer.allocation;
        details = "The requested total allocation exceeds the maximum allowed in suction.";
    }
    else
        allocation_to_kill = selectAllocationToKill(killer, getEffectiveLimit(killer), details);
    if (!allocation_to_kill)
        return;

    chassert(!allocation_to_kill->kill_requested);
    allocation_to_kill->kill_requested = true;

    SCHED_DBG("{} -- killing(allocated={}, increase_size={}, max={}, increasing={}, killing={})",
        getPath(), allocated, killer.size, max_allocated, killer.allocation.id, allocation_to_kill->id);
    allocation_to_kill->killAllocation(std::make_exception_ptr(
        Exception(ErrorCodes::RESOURCE_LIMIT_EXCEEDED,
            "Workload '{}' limit is hit for resource '{}': {}", getWorkloadName(), getResourceName(), details)));

    killer.allocation.queue.countKiller(*this);
    allocation_to_kill->queue.countVictim(*this);
}

bool AllocationLimit::setDecrease(DecreaseRequest * new_decrease)
{
    if (decrease == new_decrease)
        return false;
    decrease = new_decrease;
    return true;
}

void AllocationLimit::updateMinMaxAllocated(ResourceCost new_value)
{
    min_max_allocated = new_value;
    if (child)
        child->updateMinMaxAllocated(std::min(min_max_allocated, max_allocated));
}

}
