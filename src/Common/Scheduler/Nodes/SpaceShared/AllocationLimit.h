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
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;

private:
    bool setIncrease(IncreaseRequest * new_increase, bool reapply_constraint);
    bool setDecrease(DecreaseRequest * new_decrease);
    void selectAndKill(IncreaseRequest & killer);
    void scheduleSuction();
    void processSuction(UInt64 event_id, UInt64 observed_generation, IncreaseRequest * expected_growth);
    void clearMemoryGrowthSuspension();

    ResourceCost max_allocated = default_max_allocated;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    /// Regular growth whose first hard-limit conflict yielded to other work in this subtree.
    IncreaseRequest * suspended_growth = nullptr;
    bool suspended_growth_retry_pending = false;
    UInt64 last_seen_approval_epoch = 0;
    UInt64 memory_growth_suspension_start_epoch = 0;
    size_t memory_growth_suspension_beneficiaries = 0;

    /// Eviction is an externally authorized decision event, separate from a temporarily empty
    /// scheduling round. Scheduler-thread generations invalidate stale decisions when useful work
    /// or resource/topology state changes before the event is consumed.
    UInt64 activity_generation = 0;
    UInt64 next_suction_event_id = 0;
    UInt64 active_suction_event_id = 0;

    SpaceSharedNodePtr child;
};

}
