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
    /// touches `increase`/`decrease`, so it cannot stall admission. Does not evaluate while a decrease is
    /// pending below (that would use a stale, too-large `allocated`); the trailing call in
    /// `approveDecrease` re-evaluates once the release is applied. Must be called with no
    /// AllocationQueue mutex held (it descends into `selectAllocationToSpill`, which locks the queue).
    void checkSoftLimit();

    ResourceCost max_allocated = default_max_allocated;
    ResourceCost soft_limit = default_max_allocated; /// Spill threshold; `>= max_allocated` means disabled.

    /// At most one spill request is outstanding under this node at a time: set true when a signal is
    /// issued, and cleared by the victim's reply (`Update::spilled`, sent via `finishSpill` or implicitly
    /// when an allocation with reported reclaimable memory leaves its queue), when the subtree is
    /// detached, or once `allocated` drops back to/below `soft_limit`. No victim pointer is stored: the
    /// reply is unambiguous because there is never more than one request outstanding in the subtree.
    /// (Nested limits can each signal a victim inside the same subtree; a reply then reopens both gates
    /// and the still-unserved limit simply re-signals on its next check, with signals coalescing on the
    /// query side.)
    bool spill_requested = false;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    SpaceSharedNodePtr child;
};

}
