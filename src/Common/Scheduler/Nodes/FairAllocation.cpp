#include <Common/Scheduler/Nodes/FairAllocation.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

FairAllocation::FairAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
    : ISpaceSharedNode(event_queue_, info_)
{}

FairAllocation::~FairAllocation() = default;

const String & FairAllocation::getTypeName() const
{
    static String type_name("fair_allocation");
    return type_name;
}

void FairAllocation::attachChild(const std::shared_ptr<ISchedulerNode> & child_base)
{
    SpaceSharedNodePtr child = std::static_pointer_cast<ISpaceSharedNode>(child_base);
    if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Can't add another child with the same path: {}",
            it->second->getPath());
    child->setParentNode(this);
    child->setUsageKey(-1, 0); // force key update
    child->setDemandKey(-1, 0); // force key update
    propagateUpdate(*child, Update()
        .setAttached(child.get())
        .setIncrease(child->increase)
        .setDecrease(child->decrease));
}

void FairAllocation::removeChild(ISchedulerNode * child_base)
{
    if (auto iter = children.find(child_base->basename); iter != children.end())
    {
        SpaceSharedNodePtr child = iter->second;
        propagateUpdate(*child, Update()
            .setDetached(child.get())
            .setIncrease(nullptr)
            .setDecrease(nullptr));
        child->setUsageKey(-1, 0); // do not leave garbage
        child->setDemandKey(-1, 0); // do not leave garbage
        child->setParentNode(nullptr);
        children.erase(iter);
    }
}

ISchedulerNode * FairAllocation::getChild(const String & child_name)
{
    if (auto iter = children.find(child_name); iter != children.end())
        return iter->second.get();
    return nullptr;
}

ResourceAllocation * FairAllocation::selectAllocationToKill(IncreaseRequest * triggering)
{
    if (running_children.empty())
        return nullptr;
    ResourceAllocation * victim = running_children.rbegin()->selectAllocationToKill(triggering);

    return victim;
}

void FairAllocation::approveIncrease()
{
    chassert(increase);
    allocated += increase->size;
    increase = nullptr;
    increase_child->approveIncrease();
    setIncrease(*increase_child, increase_child->increase);
}

void FairAllocation::approveDecrease()
{
    chassert(decrease);
    allocated -= decrease->size;
    decrease = nullptr;
    decrease_child->approveDecrease();
    setDecrease(*decrease_child, decrease_child->decrease);
}

void FairAllocation::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    bool reset_increase = false;
    if (update.attached)
    {
        allocated += update.attached->allocated;
        reset_increase = true; // child's allocation changed, we need change the key
    }
    if (update.detached)
    {
        allocated -= update.detached->allocated;
        reset_increase = true; // child's allocation changed, we need change the key
    }
    if (reset_increase || update.increase)
    {
        if (setIncrease(from_child, update.increase ? *update.increase : from_child.increase))
            update.setIncrease(increase);
        else
            update.resetIncrease();
    }
    if (update.decrease)
    {
        if (setDecrease(from_child, *update.decrease))
            update.setDecrease(decrease);
        else
            update.resetDecrease();
    }
    if (parent && update)
        propagate(std::move(update));
}

bool FairAllocation::setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase)
{
    updateKey(from_child, new_increase);

    // Update current increase request
    IncreaseRequest * old_increase = increase;
    increase_child = increasing_children.empty() ? nullptr : &*increasing_children.begin();
    increase = increase_child ? increase_child->increase : nullptr;
    return old_increase != increase;
}

bool FairAllocation::setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease)
{
    updateKey(from_child, from_child.increase);

    // Update intrusive list of decreasing children
    if (from_child.isDecreasing())
    {
        if (!new_decrease)
            decreasing_children.erase(decreasing_children.iterator_to(from_child));
    }
    else if (new_decrease)
        decreasing_children.push_back(from_child);

    // Update current decrease request
    DecreaseRequest * old_decrease = decrease;
    decrease_child = decreasing_children.empty() ? nullptr : &*decreasing_children.begin();
    decrease = decrease_child ? decrease_child->decrease : nullptr;
    return old_decrease != decrease;
}

void FairAllocation::updateKey(ISpaceSharedNode & from_child, IncreaseRequest * new_increase)
{
    // Key calculation follows several principles.
    // - Isolation: We take into account increase request size to make sure huge increase request will kill itself, not allocations from other workloads.
    // - Weights: We normalize by weight to achieve fair division according to weights.
    // - Pending: We only take into account increase requests of non-pending allocations, because pending allocations do not consume resources yet.
    ResourceCost increase_size = new_increase && !new_increase->pending_allocation ? new_increase->size : 0;
    double new_key = double(from_child.allocated + increase_size) / from_child.info.weight;
    if (!from_child.usageKeyEquals(new_key))
    {
        SCHED_DBG("{} -- updateKey(from_child={}, new_increase={}): key = {}, allocated = {}, weight = {}, increase.size = {}",
            getPath(), from_child.basename, new_increase ? new_increase->allocation.id : String(),
            new_key, from_child.allocated, from_child.info.weight, increase_size);

        // Remove from intrusive sets to update the key
        if (from_child.isIncreasing())
            increasing_children.erase(increasing_children.iterator_to(from_child));
        if (from_child.isRunning())
            running_children.erase(running_children.iterator_to(from_child));

        from_child.setUsageKey(new_key, ++tie_breaker);

        // Reinsert into intrusive sets
        if (new_increase)
            increasing_children.insert(from_child);
        if (from_child.allocated > 0)
            running_children.insert(from_child);
    }
    else // The key has not been changed - do less work if possible (this is the common case on approve)
    {
        if (!from_child.isIncreasing())
        {
            if (new_increase)
                increasing_children.insert(from_child);
        }
        else if (!new_increase)
            increasing_children.erase(increasing_children.iterator_to(from_child));

        if (!from_child.isRunning())
        {
            if (from_child.allocated > 0)
                running_children.insert(from_child);
        }
        else if (from_child.allocated == 0)
            running_children.erase(running_children.iterator_to(from_child));
    }
}

}
