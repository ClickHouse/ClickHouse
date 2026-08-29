#include <Common/Scheduler/Nodes/SpaceShared/PrecedenceAllocation.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

PrecedenceAllocation::PrecedenceAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
    : ISpaceSharedNode(event_queue_, info_)
{}

PrecedenceAllocation::~PrecedenceAllocation()
{
    // We need to clear `parent` in children to avoid dangling references
    while (!children.empty())
        removeChild(children.begin()->second.get());
}

std::string_view PrecedenceAllocation::getTypeName() const { return "precedence_allocation"; }

void PrecedenceAllocation::attachChild(const std::shared_ptr<ISchedulerNode> & child_base)
{
    SpaceSharedNodePtr child = std::static_pointer_cast<ISpaceSharedNode>(child_base);
    if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Can't add another child with the same path: {}",
            it->second->getPath());
    child->setParentNode(this);
    child->updateMinMaxAllocated(min_max_allocated);
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
        child->updateMinMaxAllocated(std::numeric_limits<ResourceCost>::max());
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
    // The victim is always the least-precedence running child (the tail of `running_children`).
    // Cases to consider:
    // 1. Killer is not part of this node (`&killer != increase`):
    //    - the decision to kill inside this subtree was already taken by a parent.
    //    - just propagate down to the least precedence child (victim).
    // 2. Killer is part of this node but from a different child than the victim child:
    //    - this node is the least common ancestor of killer and victim, so precedence must be
    //      enforced here. A running allocation may reclaim from equal-or-lower precedence
    //      children, while a pending allocation may reclaim only from strictly lower precedence
    //      ones (it is not running yet, so it must not displace an already-admitted peer).
    //      In either case it must never evict a strictly higher-precedence child.
    // 3. Killer is part of the victim child:
    //    - we are above the least common ancestor; propagate down, the decision is taken lower.
    ISpaceSharedNode * killer_child = nullptr;
    for (ISchedulerNode * node = &killer.allocation.queue; node && node->parent; node = node->parent)
    {
        if (node->parent == this)
        {
            killer_child = static_cast<ISpaceSharedNode *>(node);
            break;
        }
    }

    /// Search all policy-eligible children from lowest to highest precedence. The killer branch is
    /// derived from the hierarchy rather than the currently visible increase, because suction runs
    /// while that increase is intentionally parked.
    for (auto it = running_children.rbegin(); it != running_children.rend(); ++it)
    {
        ISpaceSharedNode & victim_child = *it;
        if (killer_child && killer_child != &victim_child)
        {
            const bool victim_higher = victim_child.info.precedence < killer_child->info.precedence;
            const bool victim_equal = victim_child.info.precedence == killer_child->info.precedence;
            if (victim_higher || (victim_equal && killer.kind == IncreaseRequest::Kind::Pending))
                continue;
        }
        if (ResourceAllocation * victim = victim_child.selectAllocationToKill(killer, limit, details))
            return victim;
    }
    return nullptr;
}

UnusedCapacityReclaimResult PrecedenceAllocation::reclaimUnusedCapacity(
    IncreaseRequest & requester, ResourceCost max_size, bool allow_local_handoff)
{
    ISpaceSharedNode * requester_child = nullptr;
    for (ISchedulerNode * node = &requester.allocation.queue; node && node->parent; node = node->parent)
    {
        if (node->parent == this)
        {
            requester_child = static_cast<ISpaceSharedNode *>(node);
            break;
        }
    }

    if (increase_child && increase_child->increase == &requester)
    {
        auto reclaimed = increase_child->reclaimUnusedCapacity(requester, max_size, allow_local_handoff);
        if (reclaimed)
            return reclaimed;
    }

    /// Lower-precedence children surrender unused reservation slack first. Live allocation and
    /// admission ordering are unchanged because the release follows the ordinary decrease path.
    /// Commit one decrease per scheduler round so a changed request cannot strip unrelated slack.
    for (auto it = running_children.rbegin(); it != running_children.rend(); ++it)
    {
        if (&*it != increase_child)
        {
            if (requester_child && &*it != requester_child
                && it->info.precedence < requester_child->info.precedence)
                continue;
            const bool child_allows_local_handoff = allow_local_handoff
                && (!requester_child
                    || &*it == requester_child
                    || it->info.precedence == requester_child->info.precedence);
            auto reclaimed = it->reclaimUnusedCapacity(requester, max_size, child_allows_local_handoff);
            if (reclaimed)
                return reclaimed;
        }
    }
    return {};
}

bool PrecedenceAllocation::hasUnusedCapacityReclaimPending() const
{
    for (const auto & [_, child] : children)
    {
        if (child->hasUnusedCapacityReclaimPending())
            return true;
    }
    return false;
}

bool PrecedenceAllocation::isUnusedCapacityReclaimBeneficiary(const IncreaseRequest & request) const
{
    return increase_child && increase_child->increase == &request
        && increase_child->isUnusedCapacityReclaimBeneficiary(request);
}

bool PrecedenceAllocation::hasUnusedCapacityReclaimBeneficiary() const
{
    return std::any_of(children.begin(), children.end(), [](const auto & item)
    {
        return item.second->hasUnusedCapacityReclaimBeneficiary();
    });
}

void PrecedenceAllocation::expireUnusedCapacityReclaimBeneficiariesExcept(const IncreaseRequest & selected)
{
    if (increase_child
        && increase_child->increase
        && increase_child->increase != &selected
        && increase_child->isUnusedCapacityReclaimBeneficiary(*increase_child->increase))
    {
        ISpaceSharedNode * beneficiary_child = increase_child;
        beneficiary_child->expireUnusedCapacityReclaimBeneficiariesExcept(selected);
        setIncrease(*beneficiary_child, beneficiary_child->increase, false);
    }
}

IncreaseRequest * PrecedenceAllocation::selectFittingIncreaseForHandoff(ResourceCost max_size)
{
    /// A released-capacity hint is not permission to cross a precedence boundary. Recurse only
    /// through the child already selected after applying suspended-branch barriers; a nested Fair
    /// policy in that same branch may still park an oversized head for this one exact handoff.
    if (!increase_child || !increase || increase->allocation.isIncreaseSuspended())
        return nullptr;
    IncreaseRequest * selected = increase_child->selectFittingIncreaseForHandoff(max_size);
    if (selected)
        increase = selected;
    return selected;
}

void PrecedenceAllocation::clearFittingIncreaseForHandoff(const IncreaseRequest & request)
{
    ISpaceSharedNode * handoff_child = nullptr;
    for (ISchedulerNode * node = &request.allocation.queue; node && node->parent; node = node->parent)
    {
        if (node->parent == this)
        {
            handoff_child = static_cast<ISpaceSharedNode *>(node);
            handoff_child->clearFittingIncreaseForHandoff(request);
            break;
        }
    }
    if (handoff_child)
        setIncrease(*handoff_child, handoff_child->increase, false);
    else
        updateIncreaseSelection();
}

void PrecedenceAllocation::notifyUnusedCapacityReclaimStarted()
{
    if (updateIncreaseSelection() && parent)
        propagate(Update().setIncrease(increase));
    ISpaceSharedNode::notifyUnusedCapacityReclaimStarted();
}

void PrecedenceAllocation::notifyUnusedCapacityReclaimCompleted()
{
    bool changed = updateIncreaseSelection();
    changed |= expireLowerPrecedenceBeneficiaries();
    if (changed && parent)
        propagate(Update().setIncrease(increase));
    ISpaceSharedNode::notifyUnusedCapacityReclaimCompleted();
}

bool PrecedenceAllocation::expireLowerPrecedenceBeneficiaries()
{
    bool changed = false;
    ISpaceSharedNode * selected_child = increase_child;
    IncreaseRequest * selected_request = increase;
    if (selected_child && selected_request)
    {
        /// A lower-precedence exact handoff has now had its one local policy opportunity. Once this
        /// node chooses a concrete higher-precedence request, expire only those losing beneficiary
        /// paths; equal-precedence releases remain eligible under their normal policy.
        for (auto & [_, child] : children)
        {
            if (child.get() == selected_child
                || child->info.precedence <= selected_child->info.precedence
                || !child->increase
                || !child->isUnusedCapacityReclaimBeneficiary(*child->increase))
                continue;
            child->expireUnusedCapacityReclaimBeneficiariesExcept(*selected_request);
            changed |= setIncrease(*child, child->increase, false);
        }
    }
    return changed;
}

void PrecedenceAllocation::approveIncrease()
{
    chassert(increase);
    SCHED_DBG("{} -- approveIncrease(child={}, id={}, size={})",
        getPath(), increase_child->basename, increase->allocation.id, increase->size);
    apply(*increase);
    if (!increase_child->isRunning()) // We are adding the first allocation
        running_children.insert(*increase_child);
    increase = nullptr;
    increase_child->approveIncrease();

    setIncrease(*increase_child, increase_child->increase, false);
}

void PrecedenceAllocation::approveDecrease()
{
    chassert(decrease);
    SCHED_DBG("{} -- approveDecrease(child={}, id={}, size={})",
        getPath(), decrease_child->basename, decrease->allocation.id, decrease->size);
    apply(*decrease);
    chassert(decrease_child->isRunning());
    // Check if we are removing the last running allocation of the child
    if (decrease->removing_allocation && decrease_child->allocations == 1)
        running_children.erase(running_children.iterator_to(*decrease_child));
    decrease = nullptr;
    decrease_child->approveDecrease();
    setDecrease(*decrease_child, decrease_child->decrease, false);
    updateIncreaseSelection();
    expireLowerPrecedenceBeneficiaries();
}

void PrecedenceAllocation::retrySuspendedIncreases()
{
    for (auto & [_, child] : children)
        child->retrySuspendedIncreases();
}

bool PrecedenceAllocation::hasSuspendedIncrease() const
{
    return std::any_of(children.begin(), children.end(), [](const auto & item)
    {
        return item.second->hasSuspendedIncrease();
    });
}

void PrecedenceAllocation::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    SCHED_DBG("{} -- propagateUpdate(from_child={}, update={})", getPath(), from_child.basename, update.toString());
    apply(update);
    if (update.attached)
    {
        if (!from_child.isRunning() && from_child.allocations > 0)
            running_children.insert(from_child);
    }
    if (update.detached)
    {
        if (from_child.isRunning() && (update.detached == &from_child || from_child.allocations == 0))
            running_children.erase(running_children.iterator_to(from_child));
    }
    if (update.increase)
    {
        if (setIncrease(from_child, update.increase ? *update.increase : from_child.increase, update.detached == &from_child))
            update.setIncrease(increase);
        else
            update.resetIncrease();
    }
    if (update.decrease)
    {
        if (setDecrease(from_child, *update.decrease, update.detached == &from_child))
            update.setDecrease(decrease);
        else
            update.resetDecrease();
    }
    if (parent && update)
        propagate(std::move(update));
}

bool PrecedenceAllocation::setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase, bool detach_child)
{
    // Update intrusive sets of increasing children
    if (from_child.isIncreasing())
    {
        if (!new_increase || detach_child)
            increasing_children.erase(increasing_children.iterator_to(from_child));
    }
    else if (new_increase && !detach_child)
        increasing_children.insert(from_child);

    return updateIncreaseSelection(detach_child ? &from_child : nullptr);
}

bool PrecedenceAllocation::updateIncreaseSelection(const ISpaceSharedNode * ignored_child)
{
    // Update current increase request
    IncreaseRequest * old_increase = increase;
    auto eligible = std::find_if(increasing_children.begin(), increasing_children.end(), [](const ISpaceSharedNode & child)
    {
        return child.increase && !child.increase->allocation.isIncreaseSuspended();
    });
    auto beneficiary = eligible == increasing_children.end()
        ? increasing_children.end()
        : std::find_if(increasing_children.begin(), increasing_children.end(), [&](const ISpaceSharedNode & child)
        {
            return child.info.precedence == eligible->info.precedence
                && child.increase
                && !child.increase->allocation.isIncreaseSuspended()
                && child.isUnusedCapacityReclaimBeneficiary(*child.increase);
        });
    increase_child = beneficiary != increasing_children.end()
        ? &*beneficiary
        : (eligible == increasing_children.end() ? nullptr : &*eligible);

    /// A child with an active local reclaim round owns one scheduling turn at its precedence.
    /// Strictly higher-precedence work still wins; equal/lower-precedence siblings wait until the
    /// child either exposes its beneficiary or explicitly completes the round.
    ISpaceSharedNode * pending_local_child = nullptr;
    for (const auto & [_, child] : children)
    {
        if (child.get() != ignored_child
            && child->hasUnusedCapacityReclaimPending()
            && (!pending_local_child || child->info.precedence < pending_local_child->info.precedence))
            pending_local_child = child.get();
    }
    const bool selected_beneficiary = increase_child
        && increase_child->increase
        && increase_child->isUnusedCapacityReclaimBeneficiary(*increase_child->increase);
    const bool beneficiary_respects_pending = pending_local_child
        && selected_beneficiary
        && increase_child->info.precedence <= pending_local_child->info.precedence;
    if (!beneficiary_respects_pending
        && pending_local_child
        && (!increase_child || pending_local_child->info.precedence <= increase_child->info.precedence))
        increase_child = nullptr;

    /// Suspension is an internal reclaim state, not permission to cross a workload-policy boundary.
    /// A parked branch therefore remains a barrier to strictly lower-precedence children.
    for (const auto & [_, child] : children)
    {
        if (child.get() != ignored_child
            && child->hasSuspendedIncrease()
            && (!increase_child || child->info.precedence < increase_child->info.precedence))
        {
            increase_child = nullptr;
            break;
        }
    }
    increase = increase_child ? increase_child->increase : nullptr;
    return old_increase != increase;
}

bool PrecedenceAllocation::setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease, bool detach_child)
{
    // Update intrusive list of decreasing children
    if (from_child.isDecreasing())
    {
        if (!new_decrease || detach_child)
            decreasing_children.erase(decreasing_children.iterator_to(from_child));
    }
    else if (new_decrease && !detach_child)
        decreasing_children.push_back(from_child);

    // Update current decrease request
    DecreaseRequest * old_decrease = decrease;
    decrease_child = decreasing_children.empty() ? nullptr : &*decreasing_children.begin();
    decrease = decrease_child ? decrease_child->decrease : nullptr;
    return old_decrease != decrease;
}

void PrecedenceAllocation::updateMinMaxAllocated(ResourceCost new_value)
{
    min_max_allocated = new_value;
    for (auto & [name, child] : children)
        child->updateMinMaxAllocated(min_max_allocated);
}

}
