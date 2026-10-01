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
    if (recovery_reserve_owner && allocated <= getOrdinaryLimit(*recovery_reserve_owner))
        recovery_reserve_owner = nullptr;
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
    apply(*increase);
    increase = nullptr;
    child->approveIncrease();
    setIncrease(child->increase, false);
}

void AllocationLimit::approveDecrease()
{
    SCHED_DBG("{} -- approveDecrease({})", getPath(), decrease->allocation.id);

    chassert(decrease);
    ResourceAllocation & decreased_allocation = decrease->allocation;
    const bool removes_reserve_owner
        = recovery_reserve_owner == &decreased_allocation && decrease->removing_allocation;

    apply(*decrease);

    if (recovery_reserve_owner
        && (removes_reserve_owner || allocated <= getOrdinaryLimit(*recovery_reserve_owner)))
        recovery_reserve_owner = nullptr;

    // Check if allocation being killed released all its resources
    if (&decrease->allocation == allocation_to_kill && decrease->removing_allocation)
        allocation_to_kill = nullptr;

    decrease = nullptr;

    IncreaseRequest * old_increase = increase;
    child->approveDecrease();
    setDecrease(child->decrease);
    // Check if we can now process pending increase request in case it was not changed (e.g. other allocation was decreased here)
    // NOTE: if increase was changed, it is already propagated in approveDecrease()
    if (old_increase == increase && setIncrease(child->increase, true))
        propagate(Update().setIncrease(increase));
}

void AllocationLimit::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    SCHED_DBG("{} -- propagateUpdate(from_child={}, update={})", getPath(), from_child.basename, update.toString());
    chassert(&from_child == child.get());
    bool detached_reserve_owner = false;
    if (update.detached && recovery_reserve_owner)
    {
        for (ISchedulerNode * node = &recovery_reserve_owner->queue; node; node = node->parent)
        {
            if (node == update.detached)
            {
                detached_reserve_owner = true;
                break;
            }
        }
    }

    apply(update);
    if (detached_reserve_owner)
        recovery_reserve_owner = nullptr;

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
    if (!new_increase)
    {
        // There is no increase request to satisfy anymore, so forget any victim we were
        // reclaiming from. A previously issued kill remains harmless if its target has cleaned up.
        allocation_to_kill = nullptr;
    }

    if (!reapply_constraint && increase == new_increase)
        return false;

    IncreaseRequest * old_increase = increase;
    if (!new_increase)
    {
        increase = nullptr;
        return increase != old_increase;
    }

    ResourceCost effective_limit = getEffectiveLimit(*new_increase);
    const ResourceCost requested_total = allocated + new_increase->size;

    if (requested_total > effective_limit)
    {
        const bool can_use_recovery_reserve
            = isTopLevelLimit()
            && new_increase->kind == IncreaseRequest::Kind::Regular
            && new_increase->allocation.isProtectedFromEviction()
            && new_increase->allocation.getRecoveryReservedBytes() != 0;

        if (can_use_recovery_reserve)
        {
            if (!recovery_reserve_owner)
                recovery_reserve_owner = &new_increase->allocation;

            if (recovery_reserve_owner != &new_increase->allocation)
            {
                /// Another protected query is still using the reserve. Keep this increase pending
                /// until a decrease returns that capacity to the ordinary pool.
                increase = nullptr;
                return increase != old_increase;
            }

            /// The reserve owner may use the full resource limit. Beyond that point the existing
            /// eviction policy is the only fallback.
            effective_limit = max_allocated;
        }

        if (requested_total > effective_limit)
        {
            // Limit would be violated, so we have to reclaim resource.
            // Do not select a victim while a decrease is pending below: allocated still contains
            // memory that is about to be released, so the eviction may be unnecessary.
            if (!allocation_to_kill && decrease == nullptr)
            {
                String details;
                allocation_to_kill = selectAllocationToKill(*new_increase, effective_limit, details);
                if (allocation_to_kill)
                {
                    SCHED_DBG("{} -- killing(allocated={}, increase_size={}, max={}, increasing={}, killing={})",
                        getPath(), allocated, new_increase->size, effective_limit, new_increase->allocation.id, allocation_to_kill->id);
                    allocation_to_kill->killAllocation(std::make_exception_ptr(
                        Exception(ErrorCodes::RESOURCE_LIMIT_EXCEEDED,
                            "Workload '{}' limit is hit for resource '{}': {}", getWorkloadName(), getResourceName(), details)));

                    new_increase->allocation.queue.countKiller(*this);
                    allocation_to_kill->queue.countVictim(*this);
                }
            }
            increase = nullptr;
        }
        else
            increase = child->increase;
    }
    else
        increase = child->increase;

    return increase != old_increase;
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

ResourceCost AllocationLimit::getOrdinaryLimit(const ResourceAllocation & allocation) const
{
    const UInt64 reserved = allocation.getRecoveryReservedBytes();
    if (reserved == 0)
        return max_allocated;
    if (reserved >= static_cast<UInt64>(max_allocated))
        return 0;
    return max_allocated - static_cast<ResourceCost>(reserved);
}

ResourceCost AllocationLimit::getEffectiveLimit(const IncreaseRequest & request) const
{
    if (!isTopLevelLimit() || recovery_reserve_owner == &request.allocation)
        return max_allocated;
    return getOrdinaryLimit(request.allocation);
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
