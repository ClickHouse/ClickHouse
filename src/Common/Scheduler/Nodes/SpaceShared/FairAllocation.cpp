#include <Common/Scheduler/Nodes/SpaceShared/FairAllocation.h>
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

ResourceAllocation * FairAllocation::selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details)
{
    // Cases to consider:
    // 1. Killer is not part of this node.
    //    - decision to kill was already taken by the parent.
    //    - propagate down to the largest child (victim).
    // 2. Killer is part of this node but from a different child than the victim child.
    //    - this node is the least common ancestor of killer and victim.
    //    - we should enforce fairness rules and prevent killing that make allocation less fair.
    //    - computing it exactly is expensive and tricky, so we only distinguish two cases:
    //      a) running allocation always kills.
    //      b) pending allocation never kills. // TODO(serxa): this should be improved for better isolation
    // 3. Killer is part of the victim child.
    //    - we are above the least common ancestor of killer and victim.
    //    - propagate down, decision will be taken lower in the tree.

    if (running_children.empty())
        return nullptr;
    ISpaceSharedNode & victim_child = *running_children.rbegin();

    // Case 2b. Different children pending allocation never kill each other to avoid thrashing.
    // Keep it simple for now. Otherwise, allocations from different children may keep killing each other.
    // TODO(serxa): More sophisticated handling and tolerance rules preventing thrashing are required.
    // These rules should take into account:
    // - fair shares based on `limit` (expensive to compute in a hierarchy);
    // - and accumulated unfairness in term of ResourceCost * Time (requires tracking time).
    // - tolerance thresholds limiting the unfairness.
    if (killer.kind == IncreaseRequest::Kind::Pending && &killer == increase && victim_child.increase != &killer)
        return nullptr;

    /// Kill the allocation from the largest child. It is the last as the set is ordered by usage.
    return victim_child.selectAllocationToKill(killer, limit, details);
}

void FairAllocation::approveIncrease()
{
    chassert(increase);
    apply(*increase);
    increase = nullptr;
    increase_child->approveIncrease();
    setIncrease(*increase_child, increase_child->increase);
}

void FairAllocation::approveDecrease()
{
    chassert(decrease);
    apply(*decrease);
    decrease = nullptr;
    decrease_child->approveDecrease();
    setDecrease(*decrease_child, decrease_child->decrease);
}

void FairAllocation::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    bool reset_increase = false;
    apply(update);
    if (update.attached || update.detached)
        reset_increase = true; // child's allocation changed, we need change the key
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
    // To avoid thrashing we first server running allocation increase requests, then pending ones
    IncreaseRequest * old_increase = increase;
    increase_child = !increasing_children.empty() ?
        &*increasing_children.begin() :
        (!pending_children.empty() ? &*pending_children.begin() : nullptr);
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
    ResourceCost increase_size = new_increase && new_increase->kind != IncreaseRequest::Kind::Pending ? new_increase->size : 0;
    double new_key = double(from_child.allocated + increase_size) / from_child.info.weight;
    if (!from_child.usageKeyEquals(new_key))
    {
        SCHED_DBG("{} -- updateKey(from_child={}, new_increase={}): key = {}, allocated = {}, weight = {}, increase.size = {}",
            getPath(), from_child.basename, new_increase ? new_increase->allocation.id : String(),
            new_key, from_child.allocated, from_child.info.weight, increase_size);

        // Remove from intrusive sets to update the key
        if (from_child.isIncreasing())
            increasing_children.erase(increasing_children.iterator_to(from_child));
        if (from_child.isPending())
            pending_children.erase(pending_children.iterator_to(from_child));
        if (from_child.isRunning())
            running_children.erase(running_children.iterator_to(from_child));

        from_child.setUsageKey(new_key, ++tie_breaker);

        // Reinsert into intrusive sets
        if (new_increase)
        {
            if (new_increase->kind == IncreaseRequest::Kind::Pending)
                pending_children.insert(from_child);
            else
                increasing_children.insert(from_child);
        }
        if (from_child.allocated > 0)
            running_children.insert(from_child);
    }
    else // The key has not been changed - do less work if possible (this is the common case on approve)
    {
        if (!from_child.isIncreasing())
        {
            if (new_increase && new_increase->kind != IncreaseRequest::Kind::Pending)
                increasing_children.insert(from_child);
        }
        else if (!(new_increase && new_increase->kind != IncreaseRequest::Kind::Pending))
            increasing_children.erase(increasing_children.iterator_to(from_child));

        if (!from_child.isPending())
        {
            if (new_increase && new_increase->kind == IncreaseRequest::Kind::Pending)
                pending_children.insert(from_child);
        }
        else if (!(new_increase && new_increase->kind == IncreaseRequest::Kind::Pending))
            pending_children.erase(pending_children.iterator_to(from_child));

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
