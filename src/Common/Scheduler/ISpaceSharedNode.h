#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IncreaseRequest.h>
#include <Common/Scheduler/DecreaseRequest.h>


namespace DB
{

/// Base class for all scheduler nodes that manage space-shared resource.
class ISpaceSharedNode : public ISchedulerNode
{
public:
    explicit ISpaceSharedNode(EventQueue & event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    ISpaceSharedNode(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerNode(event_queue_, info_)
    {}

    /// NOTE: All fields and methods can only be accessed from the scheduler thread.

    ResourceCost allocated = 0; /// Currently allocated amount of resource.

    std::pair<double, size_t> parent_key{0, 0}; /// for FairAllocation: (allocated + increase.size) / weight and tie breaker
    boost::intrusive::set_member_hook<> running_hook; /// for parent: set of children with running allocations
    boost::intrusive::set_member_hook<> increasing_hook; /// for parent: set of children with pending increase request
    boost::intrusive::list_member_hook<> decreasing_hook; /// for parent: list of children with pending decrease request

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

        Update & setAttached(ISpaceSharedNode * new_attached) & noexcept { attached = new_attached; return *this; }
        Update & setDetached(ISpaceSharedNode * new_detached) & noexcept { detached = new_detached; return *this; }
        Update & setIncrease(IncreaseRequest * new_increase) & noexcept { increase = new_increase; return *this; }
        Update & setDecrease(DecreaseRequest * new_decrease) & noexcept { decrease = new_decrease; return *this; }
        Update & resetAttached() & noexcept { attached = nullptr; return *this; }
        Update & resetDetached() & noexcept { detached = nullptr; return *this; }
        Update & resetIncrease() & noexcept { increase = std::nullopt; return *this; }
        Update & resetDecrease() & noexcept { decrease = std::nullopt; return *this; }

        // To keep Update().setXXX() methods usable in rvalue context and avoid copies
        Update && setAttached(ISpaceSharedNode * new_attached) && noexcept { attached = new_attached; return std::move(*this); }
        Update && setDetached(ISpaceSharedNode * new_detached) && noexcept { detached = new_detached; return std::move(*this); }
        Update && setIncrease(IncreaseRequest * new_increase) && noexcept { increase = new_increase; return std::move(*this); }
        Update && setDecrease(DecreaseRequest * new_decrease) && noexcept { decrease = new_decrease; return std::move(*this); }
        Update && resetAttached() && noexcept { attached = nullptr; return std::move(*this); }
        Update && resetDetached() && noexcept { detached = nullptr; return std::move(*this); }
        Update && resetIncrease() && noexcept { increase = std::nullopt; return std::move(*this); }
        Update && resetDecrease() && noexcept { decrease = std::nullopt; return std::move(*this); }

        // For debugging purposes only
        String toString() const
        {
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            oss << "{ attached=" << attached << ", detached=" << detached << ", increase=";
            if (increase)
                oss << (*increase ? reinterpret_cast<void*>(&(*increase)->allocation) : nullptr);
            else
                oss << "no_change";
            oss << ", decrease=";
            if (decrease)
                oss << (*decrease ? reinterpret_cast<void*>(&(*decrease)->allocation) : nullptr);
            else
                oss << "no_change";
            oss << " }";
            return oss.str();
        }
    };

    /// Propagate updates from a child to this node.
    /// It is called when child's requests is changed or canceled (set to nullptr) or a subtree structure is changed.
    /// Recursively propagates to parents if necessary.
    virtual void propagateUpdate(ISpaceSharedNode & from_child, Update && update) = 0;

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

protected:
    ISpaceSharedNode & castParent() const
    {
        return static_cast<ISpaceSharedNode &>(*parent);
    }

    void propagate(Update && update)
    {
        castParent().propagateUpdate(*this, std::move(update));
    }
};

using SpaceSharedNodePtr = std::shared_ptr<ISpaceSharedNode>;

}
