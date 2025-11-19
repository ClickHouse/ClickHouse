#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/options.hpp>

namespace DB
{

/// Enforces a max-min fairness among its children nodes.
class FairAllocation final : public ISpaceSharedNode
{
public:
    FairAllocation(EventQueue & event_queue_, const SchedulerNodeInfo & info_);
    ~FairAllocation() override;

    // ISchedulerNode
    const String & getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_base) override;
    void removeChild(ISchedulerNode * child_base) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    ResourceAllocation * selectAllocationToKill() override;
    void approveIncrease() override;
    void approveDecrease() override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;

private:
    bool setIncrease(ISpaceSharedNode & from_child, IncreaseRequest * new_increase);
    bool setDecrease(ISpaceSharedNode & from_child, DecreaseRequest * new_decrease);
    void updateKey(ISpaceSharedNode & from_child, IncreaseRequest * new_increase);

    /// Ordering by size and unique id for tie breaking.
    /// Used for both running and increasing children for consistent ordering.
    struct CompareNodes
    {
        bool operator()(const ISpaceSharedNode & lhs, const ISpaceSharedNode & rhs) const noexcept
        {
            return lhs.parent_key < rhs.parent_key;
        }
    };

    /// Hooks for intrusive data structures
    using RunningHook    = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::running_hook>;
    using IncreasingHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::set_member_hook<>, &ISpaceSharedNode::increasing_hook>;
    using DecreasingHook = boost::intrusive::member_hook<ISpaceSharedNode, boost::intrusive::list_member_hook<>, &ISpaceSharedNode::decreasing_hook>;

    /// Intrusive data structures for managing children nodes
    /// We use intrusive structures to avoid allocations during scheduling (we might be under memory pressure)
    using RunningSet     = boost::intrusive::set<ISpaceSharedNode, RunningHook, boost::intrusive::compare<CompareNodes>>;
    using IncreasingSet  = boost::intrusive::set<ISpaceSharedNode, IncreasingHook, boost::intrusive::compare<CompareNodes>>;
    using DecreasingList = boost::intrusive::list<ISpaceSharedNode, DecreasingHook>;

    RunningSet running_children; /// Children with currently running allocations
    IncreasingSet increasing_children; /// Children with pending increase request
    DecreasingList decreasing_children; /// Children with pending decrease request
    size_t tie_breaker = 0; /// Unique id generator for tie breaking in ordering

    ISpaceSharedNode * increase_child = nullptr; /// Child that requested the current `increase`
    ISpaceSharedNode * decrease_child = nullptr; /// Child that requested the current `decrease`
    std::unordered_map<String, SpaceSharedNodePtr> children; // basename -> child
};

}
