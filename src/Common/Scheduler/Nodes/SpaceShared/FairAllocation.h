#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>

namespace DB
{

/// Enforces a max-min fairness among its children nodes.
class FairAllocation final : public ISpaceSharedNode
{
public:
    FairAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_);
    ~FairAllocation() override;

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
    void updateKey(ISpaceSharedNode & from_child, IncreaseRequest * new_increase);

    RunningSetByUsage running_children; /// Children with currently running allocations
    PendingSetByUsage pending_children; /// Children with pending allocation increase request
    IncreasingSetByUsage increasing_children; /// Children with running allocation increase request
    DecreasingList decreasing_children; /// Children with decrease request
    size_t tie_breaker = 0; /// Unique id generator for tie breaking in ordering

    ISpaceSharedNode * increase_child = nullptr; /// Child that requested the current `increase`
    ISpaceSharedNode * decrease_child = nullptr; /// Child that requested the current `decrease`
    std::unordered_map<String, SpaceSharedNodePtr> children; // basename -> child
};

}
