#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <algorithm>

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
    cancelActivation();
    resetUnusedCapacityReclaim();
    // We need to clear `parent` in child to avoid dangling references
    if (child)
        removeChild(child.get());
    /// Detach re-evaluation can enqueue this node while removeChild() is unwinding an active retry.
    /// The object is going away, so cancel that final coalesced activation as well.
    cancelActivation();
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

UnusedCapacityReclaimResult AllocationLimit::reclaimUnusedCapacity(
    IncreaseRequest & requester, ResourceCost max_size, bool allow_local_handoff)
{
    bool requester_inside = false;
    for (ISchedulerNode * node = &requester.allocation.queue; node; node = node->parent)
    {
        if (node == this)
        {
            requester_inside = true;
            break;
        }
    }

    if (!requester_inside && allow_local_handoff
        && (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
            || unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight
            || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Queued
            || unused_capacity_retry_waiting))
    {
        unused_capacity_reclaim_waiter = true;
        return {.local_demand = true};
    }

    UnusedCapacityReclaimResult reclaimed = child
        ? child->reclaimUnusedCapacity(requester, max_size, allow_local_handoff)
        : UnusedCapacityReclaimResult{};

    if (requester_inside || !allow_local_handoff || !reclaimed.decrease || reclaimed.local_demand)
        return reclaimed;

    /// A completed one-selection marker is exact and non-transferable. A second release may pass
    /// upward, but it must not overwrite the graph's still-unconsumed beneficiary.
    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary)
        return reclaimed;

    /// The committed decrease is one queued release event at this hierarchy level. Do not decide
    /// against the first decrease amount: more releases and newly exposed work may arrive before
    /// this level's one scheduler turn. Actual aggregate accounting is the source of truth.
    unused_capacity_reclaim_state = UnusedCapacityReclaimState::InFlight;
    unused_capacity_reclaim_decrease = reclaimed.decrease;
    unused_capacity_reclaim_start_pending = true;
    unused_capacity_reclaim_waiter = true;
    scheduleActivation();
    return {.local_demand = true};
}

bool AllocationLimit::hasUnusedCapacityReclaimPending() const
{
    return unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
        || unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight
        || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Queued
        || unused_capacity_retry_waiting
        || (child && child->hasUnusedCapacityReclaimPending());
}

bool AllocationLimit::isUnusedCapacityReclaimBeneficiary(const IncreaseRequest & request) const
{
    const bool local_beneficiary = unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary
        && increase == &request;
    const bool descendant_beneficiary = increase == &request
        && child
        && child->increase == &request
        && child->isUnusedCapacityReclaimBeneficiary(request);
    return local_beneficiary || descendant_beneficiary;
}

bool AllocationLimit::hasUnusedCapacityReclaimBeneficiary() const
{
    return unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary
        || (child && child->hasUnusedCapacityReclaimBeneficiary());
}

void AllocationLimit::expireUnusedCapacityReclaimBeneficiariesExcept(const IncreaseRequest & selected)
{
    bool changed = false;
    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary
        && increase != &selected)
    {
        resetUnusedCapacityReclaim();
        changed = true;
    }
    else if (child && child->increase
        && child->increase != &selected
        && child->isUnusedCapacityReclaimBeneficiary(*child->increase))
    {
        child->expireUnusedCapacityReclaimBeneficiariesExcept(selected);
        changed = true;
    }
    if (changed && child)
        setIncrease(child->increase, true, /* notify_reclaim_completion = */ false);
}

void AllocationLimit::notifyUnusedCapacityReclaimCompleted()
{
    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
        || unused_capacity_retry_waiting)
    {
        unused_capacity_reclaim_waiting_on_child = false;
        scheduleActivation();
        return;
    }
    ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
}

void AllocationLimit::notifyUnusedCapacityAvailable()
{
    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight
        || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Queued)
        return; // The exact committed decrease is the durable wakeup; coalesce later slack notices.

    IncreaseRequest * request = child ? child->increase : nullptr;
    if (request && allocatedForScheduling() + request->size > max_allocated)
    {
        /// Consume the notification at the nearest constrained limit. Stacked ancestors wake from
        /// the ordinary decrease produced here, so one release cannot start competing probes at
        /// every level before the leaf publishes its request.
        const bool completed_probe = resetUnusedCapacityReclaim();
        if (setIncrease(request, true) && parent)
            propagate(Update().setIncrease(increase));
        if (completed_probe)
            ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
        return;
    }

    if (suspended_growth)
    {
        /// The constrained request may currently be hidden by its leaf. Reopen one complete policy
        /// pass from this limit's next activation. The notification itself may be running at the
        /// tail of the leaf activation, where immediately scheduling that same leaf can coalesce.
        const bool completed_probe = resetUnusedCapacityReclaim();
        suspended_growth_retry_pending = true;
        unused_capacity_reclaim_start_pending = true;
        unused_capacity_retry_suspended = true;
        unused_capacity_retry_waiting = true;
        scheduleActivation();
        if (completed_probe)
            ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
        return;
    }

    ISpaceSharedNode::notifyUnusedCapacityAvailable();
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
        .setDecrease(child->decrease));
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
    const bool retry_round_announced = unused_capacity_retry_waiting
        && !unused_capacity_reclaim_start_pending;
    const bool completed_probe = resetUnusedCapacityReclaim();
    unused_capacity_retry_suspended = false;
    unused_capacity_retry_waiting = false;
    if (completed_probe || retry_round_announced)
        ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
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
    if (increase == suspended_growth)
        clearMemoryGrowthSuspension();
    else if (suspended_growth
        && (increase->allocation.last_increase_approval_epoch <= memory_growth_suspension_start_epoch
            || increase->allocation.last_increase_approval_epoch <= increase->allocation.last_productivity_end_epoch))
    {
        /// This allocation made its first approved progress during the current suspension.
        /// The approval epoch is written to the allocation only at the leaf, after every stacked
        /// limit has independently observed the previous value.
        ++memory_growth_suspension_beneficiaries;
    }
    last_seen_approval_epoch = increase->approval_epoch;
    const bool consumed_reclaim_handoff = isUnusedCapacityReclaimBeneficiary(*increase);
    apply(*increase);
    increase = nullptr;
    child->approveIncrease();
    /// The selected policy path must stay pinned through recursive approval. The child consumes its
    /// transient pin while approving the exact request; only then may normal policy order resume.
    if (consumed_reclaim_handoff)
        resetUnusedCapacityReclaim();
    setIncrease(child->increase, false);
}

void AllocationLimit::approveDecrease()
{
    SCHED_DBG("{} -- approveDecrease({})", getPath(), decrease->allocation.id);

    chassert(decrease);
    chassert(decrease == local_decrease);
    const bool release_had_local_turn = decrease_local_turn_complete;
    apply(*decrease);
    /// From this point the real aggregate already contains the release. Child callbacks may
    /// publish request updates while recursive approval is still unwinding, so do not subtract
    /// the same decrease virtually a second time.
    decrease_local_turn_complete = false;

    ResourceAllocation & decreased_allocation = decrease->allocation;
    const bool removed_suspended_growth = suspended_growth
        && &decreased_allocation == &suspended_growth->allocation
        && decrease->removing_allocation;
    const bool beneficiary_became_inactive = suspended_growth
        && &decreased_allocation != &suspended_growth->allocation
        && decrease->size > 0
        && decrease->size == decreased_allocation.allocated
        && decreased_allocation.last_increase_approval_epoch > memory_growth_suspension_start_epoch
        && decreased_allocation.last_increase_approval_epoch > decreased_allocation.last_productivity_end_epoch;

    // Check if allocation being killed released all its resources
    if (&decrease->allocation == allocation_to_kill && decrease->removing_allocation)
        allocation_to_kill = nullptr;
    if (removed_suspended_growth)
        clearMemoryGrowthSuspension();
    else if (suspended_growth)
    {
        if (beneficiary_became_inactive)
        {
            chassert(memory_growth_suspension_beneficiaries > 0);
            --memory_growth_suspension_beneficiaries;
        }

        if (decrease->size > 0 && !release_had_local_turn)
        {
            /// A release is a new resource-state round for every hidden request in this subtree,
            /// including requests parked in sibling queues behind policy nodes.
            suspended_growth_retry_pending = true;
            unused_capacity_reclaim_start_pending = true;
            unused_capacity_retry_suspended = true;
            unused_capacity_retry_waiting = true;
            scheduleActivation();

            if (!allocation_to_kill
                && !suspended_growth->allocation.isGrowthRecoveryActive()
                && suspended_growth->allocation.memory_growth_suction_priority)
                processSuction();
        }
    }

    IncreaseRequest * old_increase = increase;

    /// Keep the in-flight decrease visible while the child approves it. The child may propagate an
    /// updated increase before returning, and victim selection can recurse into the same queue while
    /// that queue still holds its mutex. Clearing the guard only after approval keeps suction deferred
    /// until the retry below runs outside the child lock.
    child->approveDecrease();
    local_decrease = child->decrease;
    setDecrease(nullptr);
    decrease_local_turn_complete = false;
    if (local_decrease)
    {
        if (local_decrease->size > 0)
        {
            const bool committed_decrease = unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight
                && local_decrease == unused_capacity_reclaim_decrease;
            if (unused_capacity_reclaim_state != UnusedCapacityReclaimState::InFlight || committed_decrease)
            {
                unused_capacity_reclaim_state = UnusedCapacityReclaimState::Queued;
                unused_capacity_reclaim_decrease = nullptr;
            }
            scheduleActivation();
        }
        else
        {
            decrease_local_turn_complete = true;
            setDecrease(local_decrease);
        }
    }
    // Check if we can now process pending increase request in case it was not changed (e.g. other allocation was decreased here)
    // NOTE: if increase was changed, it is already propagated in approveDecrease()
    if (!suspended_growth_retry_pending && old_increase == increase && setIncrease(child->increase, true))
        propagate(Update().setIncrease(increase));
}

void AllocationLimit::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    SCHED_DBG("{} -- propagateUpdate(from_child={}, update={})", getPath(), from_child.basename, update.toString());
    chassert(&from_child == child.get());
    bool detached_suspended_subtree = false;
    if (update.detached && suspended_growth)
    {
        for (ISchedulerNode * node = &suspended_growth->allocation.queue; node; node = node->parent)
        {
            if (node == update.detached)
            {
                detached_suspended_subtree = true;
                break;
            }
        }
    }
    const bool reclaim_round_waited = unused_capacity_reclaim_waiter
        || (!unused_capacity_reclaim_start_pending
            && (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
                || unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight));
    apply(update);
    bool reapply_constraint = false;
    bool complete_reclaim_after_update = false;
    if (update.attached)
        reapply_constraint = true;
    if (update.detached)
    {
        bool completed_probe = false;
        if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight)
        {
            /// A detach changes aggregate capacity exactly like an approved release. Keep one
            /// queued local turn and forget the request/decrease objects owned by that subtree.
            unused_capacity_reclaim_state = UnusedCapacityReclaimState::Queued;
            unused_capacity_reclaim_decrease = nullptr;
            scheduleActivation();
        }
        else if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary)
        {
            completed_probe = reclaim_round_waited;
            resetUnusedCapacityReclaim();
        }
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
        reapply_constraint = true;
        complete_reclaim_after_update = completed_probe;
    }
    // Publish the decrease BEFORE evaluating the increase: the eviction decision in `setIncrease` skips
    // victim selection while a release is in flight below, so when a single update carries both a decrease
    // and an increase, the decrease must be visible first.
    if (update.decrease)
    {
        DecreaseRequest * new_decrease = *update.decrease;
        if (local_decrease != new_decrease)
        {
            const bool parent_knew_decrease = decrease != nullptr;
            local_decrease = new_decrease;
            setDecrease(nullptr);
            decrease_local_turn_complete = false;

            if (local_decrease && local_decrease->size > 0)
            {
                const bool scheduled_release = unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled;
                const bool committed_decrease = unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight
                    && local_decrease == unused_capacity_reclaim_decrease;
                if (scheduled_release || committed_decrease
                    || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Idle
                    || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary
                    || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Exhausted)
                {
                    /// The decrease itself is the single-slot queue. Keep it below this boundary
                    /// until one local scheduler turn has observed the resulting capacity.
                    unused_capacity_reclaim_state = UnusedCapacityReclaimState::Queued;
                    unused_capacity_reclaim_decrease = nullptr;
                    unused_capacity_reclaim_waiting_on_child = false;
                }

                if (suspended_growth)
                {
                    suspended_growth_retry_pending = true;
                    unused_capacity_reclaim_start_pending = true;
                    unused_capacity_retry_suspended = true;
                    unused_capacity_retry_waiting = true;
                }
                scheduleActivation();
                update.resetDecrease();
            }
            else
            {
                /// Zero-byte removals carry lifetime/accounting information but no reusable
                /// capacity, so they do not need a local resource turn.
                decrease_local_turn_complete = true;
                setDecrease(local_decrease);
                if (parent_knew_decrease || decrease)
                    update.setDecrease(decrease);
                else
                    update.resetDecrease();
            }
        }
        else
            update.resetDecrease();
    }
    if (update.increase || reapply_constraint)
    {
        if (setIncrease(update.increase ? *update.increase : (child ? child->increase : nullptr), reapply_constraint))
            update.setIncrease(increase);
        else
            update.resetIncrease();
    }
    if (update.detached
        && (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
            || unused_capacity_reclaim_state == UnusedCapacityReclaimState::Queued
            || unused_capacity_retry_waiting))
        scheduleActivation();
    if (parent && update)
        propagate(std::move(update));
    /// Wake ancestors only after the topology update and its lower aggregate usage are visible;
    /// otherwise they could select a victim using stale pre-detach accounting.
    if (complete_reclaim_after_update)
        ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
}

bool AllocationLimit::setIncrease(
    IncreaseRequest * new_increase, bool reapply_constraint, bool notify_reclaim_completion)
{
    if (new_increase && new_increase->allocation.isIncreaseSuspended())
        new_increase = nullptr;

    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary
        && new_increase != increase)
        resetUnusedCapacityReclaim();

    /// Reconciliation may reduce a parked owner's request to zero while the allocation itself
    /// remains alive. The leaf unlinks that request before publishing the new selection; end the
    /// limit episode here so the stale embedded request cannot remain a hierarchy barrier.
    if (suspended_growth && !suspended_growth->allocation.increasing_hook.is_linked())
        clearMemoryGrowthSuspension();

    /// A retry is a FIFO barrier across every child queue. Child activations may update their
    /// cached selection during the pass, but this limit must not expose, suspend, or kill from a
    /// partial view. Phase two below evaluates the final child selection exactly once.
    if (unused_capacity_retry_waiting)
    {
        IncreaseRequest * old_increase = increase;
        increase = nullptr;
        return old_increase != increase;
    }

    /// A limit or sibling decrease can make the current request fit before its queued probe runs.
    /// Complete that round now so ancestors do not keep waiting on work which is already runnable.
    if (new_increase
        && unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
        && allocatedForScheduling() + new_increase->size <= max_allocated)
    {
        const bool completed_probe = resetUnusedCapacityReclaim();
        if (completed_probe && notify_reclaim_completion)
            ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
    }

    /// Once a descendant has committed a decrease, its bytes are no longer available here even
    /// before the leaf activation publishes the request. Keep every candidate blocked until that
    /// decrease is visible and approved; otherwise a policy update could overbook or claim twice.
    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight)
    {
        IncreaseRequest * old_increase = increase;
        increase = nullptr;
        return old_increase != increase;
    }

    if (!new_increase && !suspended_growth)
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
    if (!reapply_constraint && suspended_growth_retry_pending)
        suspended_growth_retry_pending = false;

    /// The hierarchy has exhausted every visible alternative. External suction authorizes a
    /// scheduler-thread victim search; no deferred callback or node lifetime crosses this point.
    if (!new_increase && suspended_growth && !suspended_growth_retry_pending && local_decrease == nullptr
        && !allocation_to_kill
        && !suspended_growth->allocation.isGrowthRecoveryActive()
        && suspended_growth->allocation.memory_growth_suction_priority)
        processSuction();

    if (!reapply_constraint && increase == new_increase)
        return false;
    IncreaseRequest * old_increase = increase;
    if (new_increase)
    {
        if (allocatedForScheduling() + new_increase->size > max_allocated)
        {
            /// Demand grew after the local handoff was granted. The old grant is spent; start one
            /// new exact reclaim round instead of retaining an open-ended preference.
            if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary)
                resetUnusedCapacityReclaim();

            // Limit would be violated, so we have to reclaim resource.
            // Do not select a victim while a decrease is pending below: `allocated` still contains
            // memory that is about to be released, so the eviction may be unnecessary. The increase
            // stays blocked, and every decrease approval re-runs this via `reapply_constraint`; once
            // the releases prove insufficient and no decrease is pending, the eviction fires.
            if (!allocation_to_kill && local_decrease == nullptr)
            {
                if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Idle)
                {
                    unused_capacity_reclaim_state = UnusedCapacityReclaimState::Scheduled;
                    unused_capacity_reclaim_start_pending = true;
                    scheduleActivation();
                }
                else if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Exhausted)
                {
                    /// Before evicting a running query for asking for more memory, park that growth once.
                    /// The child can then expose other work hidden behind running-query growth. Memory releases
                    /// retry the parked growth, so independent work can continue while pressure drains. If
                    /// nothing can make progress, the existing kill policy remains the fallback.
                    const bool retrying_suspended_owner = new_increase == suspended_growth;
                    bool suspended = false;
                    if (!suspended_growth)
                    {
                        suspended = new_increase->kind == IncreaseRequest::Kind::Regular
                            && new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                        if (suspended)
                        {
                            suspended_growth = new_increase;
                            /// Only allocations whose requests are approved after this point are
                            /// beneficiaries. Existing holders do not postpone the last-resort path merely
                            /// by staying alive. Approval epochs keep this hierarchy-safe for stacked limits.
                            memory_growth_suspension_start_epoch = last_seen_approval_epoch;
                            memory_growth_suspension_beneficiaries = 0;
                        }
                    }
                    else if (new_increase == suspended_growth)
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
                        suspended_growth_retry_pending = !retrying_suspended_owner;

                        SCHED_DBG("{} -- suspending increase(allocated={}, increase_size={}, max={}, allocation={})",
                            getPath(), allocated, new_increase->size, max_allocated, new_increase->allocation.id);

                        if (retrying_suspended_owner
                            && new_increase->allocation.memory_growth_suction_priority)
                            processSuction();
                    }
                    else if (!suspended_growth)
                        selectAndKill(*new_increase);
                    else if (new_increase == suspended_growth
                        && new_increase->allocation.memory_growth_suction_priority)
                        processSuction();
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

void AllocationLimit::processActivation()
{
    if (unused_capacity_reclaim_start_pending)
    {
        unused_capacity_reclaim_start_pending = false;
        ISpaceSharedNode::notifyUnusedCapacityReclaimStarted();
    }

    if (unused_capacity_retry_suspended)
    {
        unused_capacity_retry_suspended = false;
        if (child)
            child->retrySuspendedIncreases();
        /// Every child retry above is enqueued on this scheduler's FIFO event queue. Requeue this
        /// node after the broadcast, so the next activation observes one complete local pass even
        /// when policy aggregation keeps the same selected IncreaseRequest pointer.
        scheduleActivation();
        return;
    }

    if (unused_capacity_retry_waiting)
    {
        /// Nested graphs finish their own two-phase pass first. Their explicit completion wakes
        /// this node through notifyUnusedCapacityReclaimCompleted(); no timer or polling is needed.
        if (child && child->hasUnusedCapacityReclaimPending())
            return;

        /// Reconciliation may have removed the parked owner during phase one. End that episode
        /// while the barrier is still active so clearMemoryGrowthSuspension() does not enqueue a
        /// second child pass behind this phase-two event.
        if (suspended_growth && !suspended_growth->allocation.increasing_hook.is_linked())
            clearMemoryGrowthSuspension();
        unused_capacity_retry_waiting = false;
        suspended_growth_retry_pending = false;
        if (local_decrease && local_decrease->size > 0 && !decrease)
            decrease_local_turn_complete = true;
        IncreaseRequest * local_request = child ? child->increase : nullptr;
        if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Beneficiary
            && (!local_request || allocatedForScheduling() + local_request->size > max_allocated))
        {
            /// The selected path disappeared or no longer fits during the retry pass. The turn is
            /// spent; actual accounting remains available to ordinary policy.
            resetUnusedCapacityReclaim();
        }

        const bool increase_changed = setIncrease(local_request, true, /* notify_reclaim_completion = */ false);
        if (unused_capacity_reclaim_state != UnusedCapacityReclaimState::InFlight
            && local_request
            && allocatedForScheduling() + local_request->size <= max_allocated)
        {
            unused_capacity_reclaim_state = UnusedCapacityReclaimState::Beneficiary;
            unused_capacity_reclaim_decrease = nullptr;
        }
        publishDecrease(increase_changed);
        unused_capacity_reclaim_waiter = false;
        ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
        /// setIncrease() may have opened a fresh Scheduled round. Its queued activation must emit
        /// Started before probing; never fall through and consume that round in this same event.
        return;
    }

    if (local_decrease
        && local_decrease->size > 0
        && !decrease_local_turn_complete
        && unused_capacity_reclaim_state != UnusedCapacityReclaimState::Queued)
    {
        /// This release is unrelated to the exact reclaim currently in flight. It still gets one
        /// local accounting turn, but it cannot manufacture or transfer a beneficiary marker.
        decrease_local_turn_complete = true;
        const bool increase_changed = child
            && setIncrease(child->increase, true, /* notify_reclaim_completion = */ false);
        publishDecrease(increase_changed);
        return;
    }

    if (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Queued)
    {
        /// Complete this boundary's one local turn before publishing the decrease upward. Until
        /// this point neither ancestor accounting nor ancestor policy has observed the release.
        if (local_decrease && local_decrease->size > 0 && !decrease)
            decrease_local_turn_complete = true;
        const ResourceCost scheduling_allocated = allocatedForScheduling();
        const ResourceCost available = scheduling_allocated < max_allocated
            ? max_allocated - scheduling_allocated
            : 0;
        IncreaseRequest * local_request = child
            ? child->selectFittingIncreaseForHandoff(available)
            : nullptr;
        bool increase_changed = false;
        if (local_request)
        {
            increase_changed = setIncrease(local_request, true, /* notify_reclaim_completion = */ false);
            unused_capacity_reclaim_state = UnusedCapacityReclaimState::Beneficiary;
            unused_capacity_reclaim_decrease = nullptr;
        }
        else
        {
            resetUnusedCapacityReclaim();
            if (child)
                increase_changed = setIncrease(child->increase, true, /* notify_reclaim_completion = */ false);
        }
        publishDecrease(increase_changed);
        unused_capacity_reclaim_waiter = false;
        ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
        return;
    }

    if (unused_capacity_reclaim_state != UnusedCapacityReclaimState::Scheduled)
        return;

    /// A descendant owns the earlier local reclaim round. Its ordinary decrease, or its explicit
    /// no-decrease completion notification, wakes this probe without polling.
    if (local_decrease || (child && child->hasUnusedCapacityReclaimPending()))
        return;

    IncreaseRequest * request = child ? child->increase : nullptr;
    if (!request)
    {
        const bool completed_probe = resetUnusedCapacityReclaim();
        if (completed_probe)
            ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
        return;
    }

    if (allocatedForScheduling() + request->size <= max_allocated)
    {
        const bool completed_probe = resetUnusedCapacityReclaim();
        if (setIncrease(request, true) && parent)
            propagate(Update().setIncrease(increase));
        if (completed_probe)
            ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
        return;
    }

    const ResourceCost deficit = allocatedForScheduling() + request->size - max_allocated;
    UnusedCapacityReclaimResult reclaiming = child->reclaimUnusedCapacity(*request, deficit, true);
    if (reclaiming.decrease)
    {
        unused_capacity_reclaim_state = UnusedCapacityReclaimState::InFlight;
        unused_capacity_reclaim_decrease = reclaiming.decrease;
        SCHED_DBG("{} -- reclaiming unused capacity before pressure escalation"
            "(allocated={}, increase_size={}, max={}, reclaiming={})",
            getPath(), allocated, request->size, max_allocated, reclaiming.decrease->size);
        return;
    }
    if (reclaiming.local_demand)
    {
        unused_capacity_reclaim_waiting_on_child = true;
        return;
    }

    unused_capacity_reclaim_state = UnusedCapacityReclaimState::Exhausted;
    if (setIncrease(request, true) && parent)
        propagate(Update().setIncrease(increase));
    ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
    unused_capacity_reclaim_waiter = false;
}

bool AllocationLimit::resetUnusedCapacityReclaim()
{
    const bool completed_probe = unused_capacity_reclaim_waiter
        || (!unused_capacity_reclaim_start_pending
            && (unused_capacity_reclaim_state == UnusedCapacityReclaimState::Scheduled
                || unused_capacity_reclaim_state == UnusedCapacityReclaimState::InFlight));
    if (child)
        child->clearFittingIncreaseForHandoff();
    unused_capacity_reclaim_state = UnusedCapacityReclaimState::Idle;
    unused_capacity_reclaim_decrease = nullptr;
    unused_capacity_reclaim_waiter = false;
    unused_capacity_reclaim_start_pending = false;
    unused_capacity_reclaim_waiting_on_child = false;
    return completed_probe;
}

void AllocationLimit::retrySuspendedIncreases()
{
    if (child && !unused_capacity_retry_waiting)
        child->retrySuspendedIncreases();
}

void AllocationLimit::endProductiveMembership(ResourceAllocation & allocation)
{
    if (suspended_growth && &allocation != &suspended_growth->allocation
        && allocation.last_increase_approval_epoch > memory_growth_suspension_start_epoch
        && allocation.last_increase_approval_epoch > allocation.last_productivity_end_epoch)
    {
        chassert(memory_growth_suspension_beneficiaries > 0);
        --memory_growth_suspension_beneficiaries;
    }

    /// A beneficiary may be shared by stacked limits. Forward before the queue records the ending
    /// epoch so every active constraint independently observes the same transition.
    ISpaceSharedNode::endProductiveMembership(allocation);
}

bool AllocationLimit::hasSuspendedIncrease() const
{
    return suspended_growth || (child && child->hasSuspendedIncrease());
}

void AllocationLimit::clearMemoryGrowthSuspension()
{
    if (suspended_growth)
    {
        suspended_growth->allocation.onGrowthPressureResolved();
        suspended_growth->allocation.memory_growth_suction_priority = false;
    }
    suspended_growth = nullptr;
    suspended_growth_retry_pending = false;
    memory_growth_suspension_start_epoch = 0;
    memory_growth_suspension_beneficiaries = 0;
    if (child && !unused_capacity_retry_waiting)
        child->retrySuspendedIncreases();
}

void AllocationLimit::processSuction()
{
    if (!suspended_growth || suspended_growth_retry_pending || unused_capacity_retry_waiting
        || local_decrease != nullptr || allocation_to_kill
        || suspended_growth->allocation.isGrowthRecoveryActive()
        || !suspended_growth->allocation.memory_growth_suction_priority)
        return;
    selectAndKill(*suspended_growth);
}


void AllocationLimit::selectAndKill(IncreaseRequest & killer)
{
    String details;
    /// Pass the productive-beneficiary boundary through the existing hierarchy without allocating
    /// a side container under memory pressure. Every policy node can search past protected work.
    killer.allocation.memory_growth_candidate_protection_epoch = memory_growth_suspension_start_epoch;
    allocation_to_kill = selectAllocationToKill(killer, max_allocated, details);
    killer.allocation.memory_growth_candidate_protection_epoch = 0;
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

ResourceCost AllocationLimit::allocatedForScheduling() const
{
    if (decrease_local_turn_complete && local_decrease && local_decrease->size > 0)
    {
        chassert(allocated >= local_decrease->size);
        return allocated - local_decrease->size;
    }
    return allocated;
}

void AllocationLimit::publishDecrease(bool increase_changed)
{
    Update update;
    if (local_decrease && decrease != local_decrease)
    {
        chassert(decrease_local_turn_complete || local_decrease->size == 0);
        setDecrease(local_decrease);
        update.setDecrease(decrease);
    }
    if (increase_changed)
        update.setIncrease(increase);
    if (parent && update)
        propagate(std::move(update));
}

void AllocationLimit::updateMinMaxAllocated(ResourceCost new_value)
{
    min_max_allocated = new_value;
    if (child)
        child->updateMinMaxAllocated(std::min(min_max_allocated, max_allocated));
}

}
