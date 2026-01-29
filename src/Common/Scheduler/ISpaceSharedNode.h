#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IncreaseRequest.h>
#include <Common/Scheduler/DecreaseRequest.h>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/options.hpp>


namespace DB
{

/// Base class for all scheduler nodes that manage space-shared resource.
/// NOTE: All fields and methods can only be accessed from the scheduler thread.
class ISpaceSharedNode : public ISchedulerNode
{
public:
    explicit ISpaceSharedNode(EventQueue & event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    ISpaceSharedNode(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerNode(event_queue_, info_)
    {}

    ResourceCost allocated = 0; /// Currently allocated amount of resource under this node.
    size_t allocations = 0; /// Number of currently running allocations under this node.

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
            oss << "{ attached=" << (attached ? attached->getPath() : "nullptr")
                << ", detached=" << (detached ? detached->getPath() : "nullptr")
                << ", increase=" << (increase ? (*increase ? (*increase)->allocation.id : "nullptr") : "no_change")
                << ", decrease=" << (decrease ? (*decrease ? (*decrease)->allocation.id : "nullptr") : "no_change")
                << " }";
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

    /// Returns allocation to be killed from this node or its children to approve a `killer` increase request.
    /// NOTE: It is important to keep killing order opposite to acquire ordering.
    /// This means that allocation policies of every node should have:
    ///     -- acquire order -->
    ///  A0 A1 A2 A3 A4 A5 A6 A7 A8 - ResourceAllocations
    ///    <-- killing order --
    virtual ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) = 0;
    /// For parent only. Sets the usage key.
    void setUsageKey(double value, size_t tie_breaker)
    {
        chassert(!running_hook.is_linked());
        chassert(!increasing_hook.is_linked());
        usage_key.first = value;
        usage_key.second = tie_breaker;
    }

    /// For parent only. Checks the usage key.
    bool usageKeyEquals(double value) const
    {
        return usage_key.first == value;
    }

    /// For parent only. Checks the child state.
    bool isPending() const noexcept { return pending_hook.is_linked(); }
    bool isRunning() const noexcept { return running_hook.is_linked(); }
    bool isIncreasing() const noexcept { return increasing_hook.is_linked(); }
    bool isDecreasing() const noexcept { return decreasing_hook.is_linked(); }

    void apply(Update & update)
    {
        if (update.attached)
        {
            allocated += update.attached->allocated;
            allocations += update.attached->allocations;
        }
        if (update.detached)
        {
            allocated -= update.detached->allocated;
            allocations -= update.detached->allocations;
        }
        ++updates;
    }

    void apply(IncreaseRequest & request)
    {
        allocated += request.size;
        ++increases;
        if (request.kind == IncreaseRequest::Kind::Initial || request.kind == IncreaseRequest::Kind::Pending)
        {
            // Note this results in a lag for Kind::Initial.
            // They are admitted earlier, but we count them here to avoid unnecessary complexity.
            ++allocations;
            ++admits;
        }
    }

    void apply(DecreaseRequest & request)
    {
        allocated -= request.size;
        ++decreases;
        if (request.removing_allocation)
        {
            --allocations;
            ++removes;
        }
    }

    void countKiller(ISpaceSharedNode & limit)
    {
        ++killers;
        if (this != &limit && parent)
            castParent().countKiller(limit);
    }

    void countVictim(ISpaceSharedNode & limit)
    {
        ++victims;
        if (this != &limit && parent)
            castParent().countVictim(limit);
    }

    /// Introspection counters
    UInt64 updates = 0;
    UInt64 increases = 0;
    UInt64 decreases = 0;
    UInt64 admits = 0;
    UInt64 removes = 0;
    UInt64 killers = 0;
    UInt64 victims = 0;

private:
    /// Hooks for intrusive data structures
    boost::intrusive::set_member_hook<> pending_hook;
    boost::intrusive::set_member_hook<> running_hook;
    boost::intrusive::set_member_hook<> increasing_hook;
    boost::intrusive::list_member_hook<> decreasing_hook;
    using PendingHook    = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::pending_hook>;
    using RunningHook    = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::running_hook>;
    using IncreasingHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::increasing_hook>;
    using DecreasingHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::list_member_hook<>, &ISpaceSharedNode::decreasing_hook>;

    /// Keys and comparators for intrusive sets
    std::pair<double, size_t> usage_key{-1, 0};  /// (allocated + increase.size) / weight and tie breaker
    struct ByUsage { bool operator()(const ISpaceSharedNode & lhs, const ISpaceSharedNode & rhs) const noexcept { return lhs.usage_key < rhs.usage_key; } };
    struct ByPrecedence { bool operator()(const ISpaceSharedNode & lhs, const ISpaceSharedNode & rhs) const noexcept { return lhs.info.precedence < rhs.info.precedence; } };

protected:
    /// Intrusive data structures for managing sets of nodes for parent nodes (e.g. PrecedenceAllocation and FairAllocation)
    /// We use intrusive structures to avoid allocations during scheduling (we might be under memory pressure)
    using RunningSetByUsage = boost::intrusive::set<ISpaceSharedNode, RunningHook, boost::intrusive::compare<ByUsage>>;
    using PendingSetByUsage = boost::intrusive::set<ISpaceSharedNode, PendingHook, boost::intrusive::compare<ByUsage>>;
    using RunningSetByPrecedence = boost::intrusive::set<ISpaceSharedNode, RunningHook, boost::intrusive::compare<ByPrecedence>>;
    using IncreasingSetByUsage = boost::intrusive::set<ISpaceSharedNode, IncreasingHook, boost::intrusive::compare<ByUsage>>;
    using IncreasingSetByPrecedence = boost::intrusive::set<ISpaceSharedNode, IncreasingHook, boost::intrusive::compare<ByPrecedence>>;
    using DecreasingList = boost::intrusive::list<ISpaceSharedNode, DecreasingHook>;

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
