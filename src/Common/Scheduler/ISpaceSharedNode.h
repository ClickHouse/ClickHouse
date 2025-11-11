#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IncreaseRequest.h>
#include <Common/Scheduler/DecreaseRequest.h>


namespace DB
{

/// Base class for all scheduler nodes that manage space-shared resource.
/// Space-shared resources process requests to increase and decrease allocations.
/// Each node presents its pending requests through `increase` and `decrease` fields.
class ISpaceSharedNode : public ISchedulerNode
{
public:
    explicit ISpaceSharedNode(EventQueue & event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    ISpaceSharedNode(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerNode(event_queue_, info_)
    {}

    /// Currently allocated amount of resource.
    /// Can only be accessed from the scheduler thread.
    ResourceCost allocated = 0;

    /// Requests to be processed next from the node or its children.
    /// Keeping these fields up-to-date is part of request processing and activation logic
    /// and MUST be done in the scheduler thread exclusively.
    IncreaseRequest * increase = nullptr;
    DecreaseRequest * decrease = nullptr;

    struct Update
    {
        ISpaceSharedNode * attached = nullptr; /// Attached node (may be not an immediate child) or nullptr if no node attached
        ISpaceSharedNode * detached = nullptr; /// Detached node (may be not an immediate child) or nullptr if no node detached
        std::optional<IncreaseRequest *> increase; /// New increase request or nullptr if no more increase requests, null_opt means no change
        std::optional<DecreaseRequest *> decrease; /// New decrease request or nullptr if no more decrease requests, null_opt means no change

        explicit operator bool() const { return attached || detached || increase || decrease; }

        Update & setAttached(ISpaceSharedNode * new_attached) { attached = new_attached; return *this; }
        Update & setDetached(ISpaceSharedNode * new_detached) { detached = new_detached; return *this; }
        Update & setIncrease(IncreaseRequest * new_increase) { increase = new_increase; return *this; }
        Update & setDecrease(DecreaseRequest * new_decrease) { decrease = new_decrease; return *this; }
        Update & resetAttached() { attached = nullptr; return *this; }
        Update & resetDetached() { detached = nullptr; return *this; }
        Update & resetIncrease() { increase = std::nullopt; return *this; }
        Update & resetDecrease() { decrease = std::nullopt; return *this; }
    };

    /// Propagate updates from a child to this node.
    /// It is called when child's requests is changed or canceled (set to nullptr) or a subtree structure is changed.
    /// Recursively propagates to parents if necessary.
    virtual void propagateUpdate(ISpaceSharedNode & from_child, Update update) = 0;

    /// Approves and removes current `increase` from this node or its children.
    /// Updates `increase` field to the next request to be processed.
    /// Recursively propagates to children if necessary.
    virtual void approveIncrease() = 0;
    virtual void approveDecrease() = 0;

    /// Returns allocation to be killed from this node or its children.
    /// NOTE: It is important to keep killing order opposite to acquire ordering.
    /// This means that allocation policies of every node should have:
    ///     -- acquire order -->
    ///  A0 A1 A2 A3 A4 A5 A6 A7 A8 - ResourceAllocations
    ///    <-- killing order --
    virtual ResourceAllocation * selectAllocationToKill() = 0;

    ISpaceSharedNode & castParent() const
    {
        return static_cast<ISpaceSharedNode &>(*parent);
    }
};

using SpaceSharedNodePtr = std::shared_ptr<ISpaceSharedNode>;

}
