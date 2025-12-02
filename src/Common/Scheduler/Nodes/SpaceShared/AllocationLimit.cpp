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
    // WARNING: We do not force eviction here in cases there is no pending increase request to simplify logic.
    // WARNING: Eventually on the first increase request the limit will be applied.
    if (setIncrease(child->increase, true))
        propagate(Update().setIncrease(increase));
}

ResourceCost AllocationLimit::getLimit() const
{
    return max_allocated;
}

const String & AllocationLimit::getTypeName() const
{
    static String type_name("allocation_limit");
    return type_name;
}

void AllocationLimit::attachChild(const std::shared_ptr<ISchedulerNode> & child_)
{
    child = std::static_pointer_cast<ISpaceSharedNode>(child_);
    child->setParentNode(this);
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
    child.reset();
}

ISchedulerNode * AllocationLimit::getChild(const String & child_name)
{
    if (child->basename == child_name)
        return child.get();
    return nullptr;
}

ResourceAllocation * AllocationLimit::selectAllocationToKill(IncreaseRequest * killer, ResourceCost limit)
{
    return child->selectAllocationToKill(killer, limit);
}

void AllocationLimit::approveIncrease()
{
    SCHED_DBG("{} -- approveIncrease({})", getPath(), increase->allocation.id);
    chassert(increase);
    allocated += increase->size;
    increase = nullptr;
    child->approveIncrease();
    setIncrease(child->increase, false);
}

void AllocationLimit::approveDecrease()
{
    SCHED_DBG("{} -- approveDecrease({})", getPath(), decrease->allocation.id);

    chassert(decrease);
    allocated -= decrease->size;

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
    bool reapply_constraint = false;
    if (update.attached)
    {
        allocated += update.attached->allocated;
        reapply_constraint = true;
    }
    if (update.detached)
    {
        allocated -= update.detached->allocated;
        // In case of a queue purge we may need to clear allocation_to_kill
        if (allocation_to_kill && update.detached == static_cast<ISpaceSharedNode *>(&allocation_to_kill->queue))
            allocation_to_kill = nullptr;
        reapply_constraint = true;
    }
    if (update.increase || reapply_constraint)
    {
        if (setIncrease(update.increase ? *update.increase : increase, reapply_constraint))
            update.setIncrease(increase);
        else
            update.resetIncrease();
    }
    if (update.decrease)
    {
        if (setDecrease(*update.decrease))
            update.setDecrease(decrease);
        else
            update.resetDecrease();
    }
    if (parent && update)
        propagate(std::move(update));
}

bool AllocationLimit::setIncrease(IncreaseRequest * new_increase, bool reapply_constraint)
{
    if (!reapply_constraint && increase == new_increase)
        return false;
    IncreaseRequest * old_increase = increase;
    if (new_increase)
    {
        if (allocated + new_increase->size > max_allocated)
        {
            // Limit would be violated, so we have to reclaim resource
            if (!allocation_to_kill)
            {
                allocation_to_kill = selectAllocationToKill(new_increase, max_allocated);
                if (allocation_to_kill)
                {
                    SCHED_DBG("{} -- killing(allocated={}, increase_size={}, max={}, increasing={}, killing={})",
                        getPath(), allocated, new_increase->size, max_allocated, new_increase->allocation.id, allocation_to_kill->id);
                    allocation_to_kill->killAllocation(std::make_exception_ptr(
                        Exception(ErrorCodes::RESOURCE_LIMIT_EXCEEDED,
                            "Resource limit exceeded"))); // TODO(serxa): add limit details, resource name, path
                }
            }
            // Block until there is enough resource to process child's increase request
            increase = nullptr;
        }
        else
            increase = child->increase; // Can safely process child's increase request
    }
    else
        increase = nullptr; // No more increase requests

    return increase != old_increase;
}

bool AllocationLimit::setDecrease(DecreaseRequest * new_decrease)
{
    if (decrease == new_decrease)
        return false;
    decrease = new_decrease;
    return true;
}

}
