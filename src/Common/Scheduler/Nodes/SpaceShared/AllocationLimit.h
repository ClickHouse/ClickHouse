#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>


namespace DB
{

/// Limits the total amount of allocated resource by all the children nodes.
/// If `evict` is true, an increase that would violate the limit initiates eviction of a running allocation.
/// Otherwise, the increase is blocked until enough resource is released (e.g. query slots are never evicted).
class AllocationLimit final : public ISpaceSharedNode
{
    static constexpr ResourceCost default_max_allocated = std::numeric_limits<ResourceCost>::max();
public:
    AllocationLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, ResourceCost max_allocated_, bool evict_ = true);
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
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;

private:
    bool setIncrease(IncreaseRequest * new_increase, bool reapply_constraint);
    bool setDecrease(DecreaseRequest * new_decrease);

    ResourceCost max_allocated = default_max_allocated;

    /// Whether running allocations may be evicted to satisfy an increase that would violate the limit
    const bool evict = true;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    SpaceSharedNodePtr child;
};

}
