#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>

namespace DB
{

/// Enforces a precedence among its children nodes.
class PrecedenceAllocation final : public ISpaceSharedNode
{
public:
    PrecedenceAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_);
    ~PrecedenceAllocation() override;

    // ISchedulerNode
    const String & getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_base) override;
    void removeChild(ISchedulerNode * child_base) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    void approveIncrease() override;
    void approveDecrease() override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;

private:
    bool setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase);
    bool setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease);

    /// Ordering by precedence. Used for both running and increasing children for consistent ordering.
    /// NOTE: According to IWorkloadNode::updateRequiresDetach() any change in precedence will lead to child
    /// NOTE: detach and reattach, thus we may assume keys to be constant.
    RunningSetByPrecedence running_children; /// Children with currently running allocations
    IncreasingSetByPrecedence increasing_children; /// Children with pending increase request
    DecreasingList decreasing_children; /// Children with pending decrease request

    ISpaceSharedNode * increase_child = nullptr; /// Child that requested the current `increase`
    ISpaceSharedNode * decrease_child = nullptr; /// Child that requested the current `decrease`
    std::unordered_map<String, SpaceSharedNodePtr> children; // basename -> child
};

}
