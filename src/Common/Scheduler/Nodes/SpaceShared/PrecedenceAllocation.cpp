#include <Common/Scheduler/Nodes/SpaceShared/PrecedenceAllocation.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

PrecedenceAllocation::PrecedenceAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
    : ISpaceSharedNode(event_queue_, info_)
{}

PrecedenceAllocation::~PrecedenceAllocation() = default;

const String & PrecedenceAllocation::getTypeName() const
{
    static String type_name("precedence_allocation");
    return type_name;
}

void PrecedenceAllocation::attachChild(const std::shared_ptr<ISchedulerNode> & child_base)
{
    SpaceSharedNodePtr child = std::static_pointer_cast<ISpaceSharedNode>(child_base);
    if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Can't add another child with the same path: {}",
            it->second->getPath());
    child->setParentNode(this);
    propagateUpdate(*child, Update()
        .setAttached(child.get())
        .setIncrease(child->increase)
        .setDecrease(child->decrease));
}

void PrecedenceAllocation::removeChild(ISchedulerNode * child_base)
{
    if (auto iter = children.find(child_base->basename); iter != children.end())
    {
        SpaceSharedNodePtr child = iter->second;
        propagateUpdate(*child, Update()
            .setDetached(child.get())
            .setIncrease(nullptr)
            .setDecrease(nullptr));
        child->setParentNode(nullptr);
        children.erase(iter);
    }
}

ISchedulerNode * PrecedenceAllocation::getChild(const String & child_name)
{
    if (auto iter = children.find(child_name); iter != children.end())
        return iter->second.get();
    return nullptr;
}

ResourceAllocation * PrecedenceAllocation::selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details)
{
    // Cases to consider:
    // 1. Killer is not part of this node:
    //    - decision to kill was already taken by the parent.
    //    - propagate down to the least precedence child (victim).
    // 2. Killer is part of this node but from a different child than the victim child.
    //    - this node is the least common ancestor of killer and victim.
    //    - enforce precedence rules: running allocation kills victims of equal and lower precedence.
    // 3. Killer is part of the victim child (thus, they have equal precedence).
    //    - we are above the least common ancestor of killer and victim.
    //    - propagate down, decision will be taken lower in the tree.
    // NOTE: All cases are automagically handled by picking the least precedence running child as victim.
    if (running_children.empty())
        return nullptr;
    ISpaceSharedNode & victim_child = *running_children.rbegin();
    return victim_child.selectAllocationToKill(killer, limit, details);
}

void PrecedenceAllocation::approveIncrease()
{
    chassert(increase);
    apply(*increase);
    if (!increase_child->isRunning()) // We are adding the first allocation
        running_children.insert(*increase_child);
    increase = nullptr;
    increase_child->approveIncrease();

    setIncrease(*increase_child, increase_child->increase);
}

void PrecedenceAllocation::approveDecrease()
{
    chassert(decrease);
    apply(*decrease);
    chassert(decrease_child->isRunning());
    if (decrease_child->allocated == decrease->size) // We are removing the last allocation
        running_children.erase(running_children.iterator_to(*decrease_child));
    decrease = nullptr;
    decrease_child->approveDecrease();
    setDecrease(*decrease_child, decrease_child->decrease);
}

void PrecedenceAllocation::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    apply(update);
    if (update.attached)
    {
        if (!from_child.isRunning() && from_child.allocated > 0)
            running_children.insert(from_child);
    }
    if (update.detached)
    {
        if (from_child.isRunning() && from_child.allocated == 0)
            running_children.erase(running_children.iterator_to(from_child));
    }
    if (update.increase)
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

bool PrecedenceAllocation::setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase)
{
    // Update intrusive sets of increasing children
    if (from_child.isIncreasing())
    {
        if (!new_increase)
            increasing_children.erase(increasing_children.iterator_to(from_child));
    }
    else if (new_increase)
        increasing_children.insert(from_child);

    // Update current increase request
    IncreaseRequest * old_increase = increase;
    increase_child = increasing_children.empty() ? nullptr : &*increasing_children.begin();
    increase = increase_child ? increase_child->increase : nullptr;
    return old_increase != increase;
}

bool PrecedenceAllocation::setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease)
{
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

}
