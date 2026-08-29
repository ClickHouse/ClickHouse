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
    UnusedCapacityReclaimResult reclaimUnusedCapacity(
        IncreaseRequest & requester, ResourceCost max_size, bool allow_local_handoff) override;
    bool hasUnusedCapacityReclaimPending() const override;
    bool isUnusedCapacityReclaimBeneficiary(const IncreaseRequest & request) const override;
    bool hasUnusedCapacityReclaimBeneficiary() const override;
    void expireUnusedCapacityReclaimBeneficiariesExcept(const IncreaseRequest & selected) override;
    void notifyUnusedCapacityReclaimCompleted() override;
    void notifyUnusedCapacityAvailable() override;
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    void approveIncrease() override;
    void approveDecrease() override;
    void retrySuspendedIncreases() override;
    void endProductiveMembership(ResourceAllocation & allocation) override;
    bool hasSuspendedIncrease() const override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;
    void processActivation() override;

private:
    bool setIncrease(
        IncreaseRequest * new_increase, bool reapply_constraint, bool notify_reclaim_completion = true);
    bool setDecrease(DecreaseRequest * new_decrease);
    void selectAndKill(IncreaseRequest & killer);
    void processSuction();
    void clearMemoryGrowthSuspension();
    bool resetUnusedCapacityReclaim();

    enum class UnusedCapacityReclaimState : UInt8
    {
        Idle,
        Scheduled,
        Queued,
        InFlight,
        Beneficiary,
        Exhausted,
    };

    ResourceCost max_allocated = default_max_allocated;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    /// Regular growth whose first hard-limit conflict yielded to other work in this subtree.
    IncreaseRequest * suspended_growth = nullptr;
    bool suspended_growth_retry_pending = false;
    UInt64 last_seen_approval_epoch = 0;
    UInt64 memory_growth_suspension_start_epoch = 0;
    size_t memory_growth_suspension_beneficiaries = 0;

    /// A reclaim probe is deferred to a scheduler activation so it never re-enters a queue whose
    /// mutex is held by the update currently reaching this limit.
    UnusedCapacityReclaimState unused_capacity_reclaim_state = UnusedCapacityReclaimState::Idle;
    DecreaseRequest * unused_capacity_reclaim_decrease = nullptr;
    /// True after this node returned `local_demand` and an ancestor is waiting for a durable
    /// decrease or explicit completion. This is distinct from the deferred policy Start event.
    bool unused_capacity_reclaim_waiter = false;
    bool unused_capacity_reclaim_start_pending = false;
    /// This level yielded to a nearer dependency graph's local release round. Ordinary child
    /// decreases still propagate through us, but cannot be claimed here until that round ends.
    bool unused_capacity_reclaim_waiting_on_child = false;
    bool unused_capacity_retry_suspended = false;
    bool unused_capacity_retry_waiting = false;

    SpaceSharedNodePtr child;
};

}
