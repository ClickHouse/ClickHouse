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
    std::string_view getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_base) override;
    void removeChild(ISchedulerNode * child_base) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    UnusedCapacityReclaimResult reclaimUnusedCapacity(
        IncreaseRequest & requester, ResourceCost max_size, bool allow_local_handoff) override;
    bool hasUnusedCapacityReclaimPending() const override;
    bool isUnusedCapacityReclaimBeneficiary(const IncreaseRequest & request) const override;
    bool hasUnusedCapacityReclaimBeneficiary() const override;
    void expireUnusedCapacityReclaimBeneficiariesExcept(const IncreaseRequest & selected) override;
    void notifyUnusedCapacityReclaimStarted() override;
    void notifyUnusedCapacityReclaimCompleted() override;
    IncreaseRequest * selectFittingIncreaseForHandoff(ResourceCost max_size) override;
    void clearFittingIncreaseForHandoff() override;
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    void approveIncrease() override;
    void approveDecrease() override;
    void retrySuspendedIncreases() override;
    bool hasSuspendedIncrease() const override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;

private:
    bool setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase, bool detach_child);
    bool updateIncreaseSelection(const ISpaceSharedNode * ignored_child = nullptr);
    bool setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease, bool detach_child);
    void updateKey(ISpaceSharedNode & from_child, IncreaseRequest * new_increase, bool detach_child);

    RunningSetByUsage running_children; /// Children with currently running allocations
    PendingSetByUsage pending_children; /// Children with pending allocation increase request
    IncreasingSetByUsage increasing_children; /// Children with running allocation increase request
    DecreasingList decreasing_children; /// Children with decrease request
    size_t tie_breaker = 0; /// Unique id generator for tie breaking in ordering

    ISpaceSharedNode * increase_child = nullptr; /// Child that requested the current `increase`
    ISpaceSharedNode * decrease_child = nullptr; /// Child that requested the current `decrease`
    ISpaceSharedNode * fitting_handoff_child = nullptr; /// Node-owned path for one released-capacity turn.
    std::unordered_map<String, SpaceSharedNodePtr> children; // basename -> child
};

}
