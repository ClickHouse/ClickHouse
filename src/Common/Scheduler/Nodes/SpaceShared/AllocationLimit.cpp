#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/IAllocationQueue.h>
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
    if (increase == suspended_growth)
    {
        suspended_growth = nullptr;
        suspended_growth_retry_pending = false;
        memory_growth_suspension_beneficiaries = 0;
        ++memory_growth_suspension_generation;
    }
    else if (suspended_growth
        && (increase->kind == IncreaseRequest::Kind::Pending || increase->kind == IncreaseRequest::Kind::Initial)
        && (increase->allocation.memory_growth_suspension_owner != this
            || increase->allocation.memory_growth_suspension_generation != memory_growth_suspension_generation))
    {
        /// This allocation was admitted only because the blocked growth yielded. Keep the suspension
        /// alive until the beneficiary releases, finishes, or becomes blocked on its own growth.
        increase->allocation.memory_growth_suspension_owner = this;
        increase->allocation.memory_growth_suspension_generation = memory_growth_suspension_generation;
        ++memory_growth_suspension_beneficiaries;
    }
    apply(*increase);
    increase = nullptr;
    child->approveIncrease();
    setIncrease(child->increase, false);
}

void AllocationLimit::approveDecrease()
{
    SCHED_DBG("{} -- approveDecrease({})", getPath(), decrease->allocation.id);

    chassert(decrease);
    apply(*decrease);

    ResourceAllocation & decreased_allocation = decrease->allocation;
    const bool removed_suspended_growth = suspended_growth
        && &decreased_allocation == &suspended_growth->allocation
        && decrease->removing_allocation;
    const bool retry_from_other_queue = suspended_growth
        && &decreased_allocation.queue != &suspended_growth->allocation.queue
        && !removed_suspended_growth;
    const bool removed_beneficiary = suspended_growth
        && decrease->removing_allocation
        && decreased_allocation.memory_growth_suspension_owner == this
        && decreased_allocation.memory_growth_suspension_generation == memory_growth_suspension_generation;

    // Check if allocation being killed released all its resources
    if (&decrease->allocation == allocation_to_kill && decrease->removing_allocation)
        allocation_to_kill = nullptr;
    if (removed_suspended_growth)
    {
        suspended_growth = nullptr;
        suspended_growth_retry_pending = false;
        memory_growth_suspension_beneficiaries = 0;
        ++memory_growth_suspension_generation;
    }
    else
    {
        if (removed_beneficiary)
        {
            chassert(memory_growth_suspension_beneficiaries > 0);
            --memory_growth_suspension_beneficiaries;
        }

        if (retry_from_other_queue)
        {
            /// The leaf that owns the parked growth cannot observe releases in sibling queues. Wake it
            /// explicitly; the retry remains event-driven and is evaluated after this decrease is applied.
            suspended_growth_retry_pending = true;
            suspended_growth->allocation.queue.retrySuspendedIncrease(suspended_growth->allocation);
        }
    }

    decrease = nullptr;

    IncreaseRequest * old_increase = increase;
    child->approveDecrease();
    setDecrease(child->decrease);
    // Check if we can now process pending increase request in case it was not changed (e.g. other allocation was decreased here)
    // NOTE: if increase was changed, it is already propagated in approveDecrease()
    if (!suspended_growth_retry_pending && old_increase == increase && setIncrease(child->increase, true))
        propagate(Update().setIncrease(increase));
}

void AllocationLimit::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    SCHED_DBG("{} -- propagateUpdate(from_child={}, update={})", getPath(), from_child.basename, update.toString());
    chassert(&from_child == child.get());
    apply(update);
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
        suspended_growth = nullptr;
        suspended_growth_retry_pending = false;
        memory_growth_suspension_beneficiaries = 0;
        ++memory_growth_suspension_generation;
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

    /// The hierarchy has exhausted every visible alternative. Keep the growth parked while work admitted
    /// by this suspension is still productive. With no beneficiary left, eviction is the last resort.
    if (!new_increase && suspended_growth && !suspended_growth_retry_pending && decrease == nullptr
        && memory_growth_suspension_beneficiaries == 0 && !allocation_to_kill)
        selectAndKill(*suspended_growth);

    if (!reapply_constraint && increase == new_increase)
        return false;
    IncreaseRequest * old_increase = increase;
    if (new_increase)
    {
        if (allocated + new_increase->size > max_allocated)
        {
            // Limit would be violated, so we have to reclaim resource.
            // Do not select a victim while a decrease is pending below: `allocated` still contains
            // memory that is about to be released, so the eviction may be unnecessary. The increase
            // stays blocked, and every decrease approval re-runs this via `reapply_constraint`; once
            // the releases prove insufficient and no decrease is pending, the eviction fires.
            if (!allocation_to_kill && decrease == nullptr)
            {
                /// Before evicting a running query for asking for more memory, park that growth once.
                /// The child can then expose other work hidden behind running-query growth. Memory releases
                /// retry the parked growth, so independent work can continue while pressure drains. If
                /// nothing can make progress, the existing kill policy remains the fallback.
                bool suspended = false;
                if (!suspended_growth)
                {
                    suspended = new_increase->kind == IncreaseRequest::Kind::Regular
                        && new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                    if (suspended)
                    {
                        suspended_growth = new_increase;
                        memory_growth_suspension_beneficiaries = 0;
                        ++memory_growth_suspension_generation;
                    }
                }
                else if (new_increase == suspended_growth)
                {
                    suspended = new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                }
                else if (&new_increase->allocation.queue == &suspended_growth->allocation.queue
                    && new_increase->kind != IncreaseRequest::Kind::Regular)
                {
                    /// Continue the same queue-local search past alternatives that do not fit.
                    suspended = new_increase->allocation.queue.trySuspendIncrease(new_increase->allocation);
                }

                if (suspended)
                {
                    /// Wait until the changed eligibility has propagated through the child policy before
                    /// interpreting a null increase as exhaustion of the suspension round.
                    suspended_growth_retry_pending = true;
                    SCHED_DBG("{} -- suspending increase(allocated={}, increase_size={}, max={}, allocation={})",
                        getPath(), allocated, new_increase->size, max_allocated, new_increase->allocation.id);
                }
                else
                    selectAndKill(*new_increase);
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

void AllocationLimit::selectAndKill(IncreaseRequest & killer)
{
    String details;
    allocation_to_kill = selectAllocationToKill(killer, max_allocated, details);
    if (!allocation_to_kill)
        return;

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
