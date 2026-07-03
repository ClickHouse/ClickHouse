#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>


namespace DB
{

/// Limits the total amount of allocated resource by all the children nodes.
class AllocationLimit final : public ISpaceSharedNode
{
    static constexpr ResourceCost default_max_allocated = std::numeric_limits<ResourceCost>::max();
public:
    AllocationLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, ResourceCost max_allocated_,
        ResourceCost soft_limit_ = default_max_allocated);
    ~AllocationLimit() override;
    void updateLimit(UInt64 new_max_allocated);
    ResourceCost getLimit() const;
    /// Sets the soft limit — the threshold above which the node asks reclaimable allocations to spill.
    /// Must run on the scheduler thread. A value >= `max_allocated` disables spilling (the default).
    void updateSoftLimit(ResourceCost new_soft_limit);
    ResourceCost getSoftLimit() const;

    // ISchedulerNode
    std::string_view getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override;
    void removeChild(ISchedulerNode * child_) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    ResourceAllocation * selectAllocationToSpill(ResourceCost at_least, String & details) override;
    void approveIncrease() override;
    void approveDecrease() override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;

private:
    bool setIncrease(IncreaseRequest * new_increase, bool reapply_constraint);
    bool setDecrease(DecreaseRequest * new_decrease);

    /// Issues at most one spill signal while `allocated` exceeds `soft_limit`. Non-blocking: it never
    /// touches `increase`/`decrease`, so it cannot stall admission (invariant I4). Must be called with no
    /// AllocationQueue mutex held (it descends into `selectAllocationToSpill`, which locks the queue).
    void checkSoftLimit();

    ResourceCost max_allocated = default_max_allocated;
    ResourceCost soft_limit = default_max_allocated; /// Spill threshold; `>= max_allocated` means disabled.

    /// One spill at a time (decision D2): true after a signal is issued, cleared once `allocated` drops
    /// back to/below `soft_limit`, on any decrease under this node, or when the subtree is detached. No
    /// victim pointer is stored, avoiding the dangling-pointer hazards that `allocation_to_kill` guards.
    bool spill_requested = false;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    SpaceSharedNodePtr child;
};

}
