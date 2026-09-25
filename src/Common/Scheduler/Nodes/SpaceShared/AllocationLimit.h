#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>


namespace DB
{

/// Limits the total amount of allocated resource by all the children nodes.
class AllocationLimit final : public ISpaceSharedNode
{
    static constexpr ResourceCost default_max_allocated = std::numeric_limits<ResourceCost>::max();
public:
    AllocationLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, ResourceCost max_allocated_);
    ~AllocationLimit() override;
    void updateLimit(UInt64 new_max_allocated);
    ResourceCost getLimit() const;

    // ISchedulerNode
    std::string_view getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override;
    void removeChild(ISchedulerNode * child_) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    void approveIncrease() override;
    void approveDecrease() override;
    void retrySuspendedIncreases() override;
    bool hasSuspendedIncrease() const override;
    ResourceAllocation * getLocalSpillingAllocation() const override;
    ResourceAllocation * getLocalSuctionAllocation() const override;
    ResourceAllocation * getSuctionAllocation() const override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;

private:
    bool setIncrease(IncreaseRequest * new_increase, bool reapply_constraint);
    bool setDecrease(DecreaseRequest * new_decrease);
    bool isTopLevelLimit() const;
    ResourceCost getEffectiveLimit(const IncreaseRequest & request) const;
    void selectAndKill(IncreaseRequest & killer);
    void processSuction();
    void clearMemoryGrowthSuspension();
    void clearSuction();
    void retrySuctionWaiters();

    ResourceCost max_allocated = default_max_allocated;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    /// The one recovery owner in this scope. `memory_growth_suction_priority` marks its final retry phase.
    IncreaseRequest * recovery_growth = nullptr;
    bool recovery_growth_retry_pending = false;

    SpaceSharedNodePtr child;
};

}
