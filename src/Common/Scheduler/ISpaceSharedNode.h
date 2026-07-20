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
///
/// Reclaimable-memory / spilling invariants (maintained by all subclasses):
///  - All `reclaimable` state (the aggregate and the reclaimable-filtered sets) is read and written
///      only on the scheduler thread; queries touch it exclusively through `IAllocationQueue::setReclaimable`.
///  - `reclaimable` is a bottom-up aggregate: each `setReclaimable` change travels one leaf-to-root path
///      as `Update::reclaimable_delta`, adding to every ancestor (cost O(depth)). It never descends.
///  - Per allocation `0 <= reclaimable <= allocated`; per node `reclaimable == sum of children reclaimable`
///      and therefore `0 <= reclaimable <= allocated`.
///  - Spilling never blocks the increase chain. Only the hard limit (`AllocationLimit::max_allocated`)
///      can null out `increase`; a soft-limit breach merely issues a `spillAllocation` signal.
///  - Spill victim selection (`selectAllocationToSpill`) touches at most one root-to-leaf path — no more
///      scheduling nodes than the existing kill path (`selectAllocationToKill`).
///  - Fail-close: if nothing on the chosen path is reclaimable, no spill is issued and the hard limit
///      remains the sole enforcement mechanism (reclaimable == 0 everywhere reproduces today's behavior).
///  - Membership: a child is in its parent's reclaimable set iff its aggregate `reclaimable > 0`; an
///      allocation is in its queue's reclaimable set iff its `reclaimable > 0`.
///  - The reclaimable set uses the SAME order as the primary running set at each node, so the spill
///      victim (top of the reclaimable set) is the same choice the kill path would make among reclaimable
///      candidates — deterministic, largest-usage / least-precedence / largest-allocation first.
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
    ResourceCost reclaimable = 0; /// Sum over children of the reclaimable portion of `allocated` that could be spilled on request (advisory).

    /// Requests to be processed next from the node or its children.
    /// Keeping these fields up-to-date is part of request processing and activation logic
    /// and MUST be done in the scheduler thread exclusively.
    IncreaseRequest * increase = nullptr;
    DecreaseRequest * decrease = nullptr;

    /// Minimum of the limits on allocated resource for all parent nodes (AllocationLimit::max_allocated).
    /// Required to reject impossible pending allocations and avoid infinite blocking.
    ResourceCost min_max_allocated = std::numeric_limits<ResourceCost>::max();

    /// Updates min_max_allocated and recursively propagates to children.
    /// Called when a child is attached or when AllocationLimit::max_allocated changes.
    virtual void updateMinMaxAllocated(ResourceCost new_value) = 0;

    struct Update
    {
        ISpaceSharedNode * attached = nullptr; /// Attached node (may be not an immediate child) or nullptr if no node attached
        ISpaceSharedNode * detached = nullptr; /// Detached node (may be not an immediate child) or nullptr if no node detached
        std::optional<IncreaseRequest *> increase; /// New increase request or nullptr if no more increase requests, null_opt means no change
        std::optional<DecreaseRequest *> decrease; /// New decrease request or nullptr if no more decrease requests, null_opt means no change
        ResourceCost reclaimable_delta = 0; /// Change to `reclaimable` to add on every node on the path to the root.

        explicit operator bool() const { return attached || detached || increase || decrease || reclaimable_delta != 0; }

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
        Update & setReclaimableDelta(ResourceCost value) & noexcept { reclaimable_delta = value; return *this; }
        Update && setReclaimableDelta(ResourceCost value) && noexcept { reclaimable_delta = value; return std::move(*this); }
        Update && resetAttached() && noexcept { attached = nullptr; return std::move(*this); }
        Update && resetDetached() && noexcept { detached = nullptr; return std::move(*this); }
        Update && resetIncrease() && noexcept { increase = std::nullopt; return std::move(*this); }
        Update && resetDecrease() && noexcept { decrease = std::nullopt; return std::move(*this); }

        // For debugging purposes only
        String toString() const
        {
            return fmt::format("{{ attached={}, detached={}, increase={}, decrease={}, reclaimable_delta={} }}",
                attached ? attached->getPath() : "nullptr",
                detached ? detached->getPath() : "nullptr",
                increase ? (*increase ? (*increase)->allocation.id : "nullptr") : "no_change",
                decrease ? (*decrease ? (*decrease)->allocation.id : "nullptr") : "no_change",
                reclaimable_delta);
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

    /// Returns an allocation that should be asked to spill (reclaim) `at_least` bytes, or nullptr if this
    /// subtree has nothing reclaimable. Selection descends the reclaimable-filtered ordering set at each
    /// node, using the SAME order as the kill path, so it visits exactly one root-to-leaf
    /// path and never touches idle/unreclaimable subtrees. Unlike `selectAllocationToKill`,
    /// this carries no cross-child fairness logic: the largest reclaimable allocation is spilled first,
    /// even if it is below its fair share. Returns nullptr when nothing on
    /// the chosen path is reclaimable, in which case no spill is issued and the hard limit governs
    /// (fail-close).
    virtual ResourceAllocation * selectAllocationToSpill(ResourceCost at_least, String & details) = 0;

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

    /// For parent only. True iff this child is currently listed in the parent's reclaimable-filtered set.
    /// This holds exactly when `reclaimable > 0` (and the child is attached).
    bool isReclaimable() const noexcept { return reclaimable_hook.is_linked(); }

    void apply(Update & update)
    {
        if (update.attached)
        {
            allocated += update.attached->allocated;
            allocations += update.attached->allocations;
            reclaimable += update.attached->reclaimable;
        }
        if (update.detached)
        {
            allocated -= update.detached->allocated;
            allocations -= update.detached->allocations;
            reclaimable -= update.detached->reclaimable;
        }
        reclaimable += update.reclaimable_delta;
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
            chassert(allocations > 0); // Must have been admitted via `apply(IncreaseRequest)` first
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
    UInt64 spills = 0; /// Number of `spillAllocation` signals issued from this node (soft-limit breaches).

private:
    /// Hooks for intrusive data structures
    boost::intrusive::set_member_hook<> pending_hook;
    boost::intrusive::set_member_hook<> running_hook;
    boost::intrusive::set_member_hook<> increasing_hook;
    boost::intrusive::list_member_hook<> decreasing_hook;
    boost::intrusive::set_member_hook<> reclaimable_hook; /// Membership in the parent's reclaimable-filtered set (linked iff `reclaimable > 0`).
    using PendingHook    = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::pending_hook>;
    using RunningHook    = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::running_hook>;
    using IncreasingHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::increasing_hook>;
    using DecreasingHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::list_member_hook<>, &ISpaceSharedNode::decreasing_hook>;
    using ReclaimableHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::reclaimable_hook>;

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

    /// Reclaimable-filtered ordering sets: a parallel set holding only children with `reclaimable > 0`,
    /// kept in the SAME order as the corresponding running set so that the spill victim (the top of the
    /// reclaimable set) matches the kill order. See `selectAllocationToSpill`.
    using ReclaimableSetByUsage = boost::intrusive::set<ISpaceSharedNode, ReclaimableHook, boost::intrusive::compare<ByUsage>>;
    using ReclaimableSetByPrecedence = boost::intrusive::set<ISpaceSharedNode, ReclaimableHook, boost::intrusive::compare<ByPrecedence>>;

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
