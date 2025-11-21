#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>

namespace DB
{

/// Enforces a priority among its children nodes.
class PriorityAllocation final : public ISpaceSharedNode
{
public:
    PriorityAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_);
    ~PriorityAllocation() override;

    // ISchedulerNode
    const String & getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_base) override;
    void removeChild(ISchedulerNode * child_base) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    ResourceAllocation * selectAllocationToKill() override;
    void approveIncrease() override;
    void approveDecrease() override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;

private:
    bool setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase);
    bool setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease);

    /// Ordering by priority. Used for both running and increasing children for consistent ordering.
    /// NOTE: According to IWorkloadNode::updateRequiresDetach() any change in priority will lead to child
    /// NOTE: detach and reattach, thus we may assume keys to be constant.
    RunningSetByPriority running_children; /// Children with currently running allocations
    IncreasingSetByPriority increasing_children; /// Children with pending increase request
    DecreasingList decreasing_children; /// Children with pending decrease request

    ISpaceSharedNode * increase_child = nullptr; /// Child that requested the current `increase`
    ISpaceSharedNode * decrease_child = nullptr; /// Child that requested the current `decrease`
    std::unordered_map<String, SpaceSharedNodePtr> children; // basename -> child
};

}
