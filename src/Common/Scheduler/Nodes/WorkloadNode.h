#pragma once

#include <Common/Exception.h>
#include <Common/Priority.h>
#include <Common/Scheduler/CostUnit.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ISpaceSharedNode.h>
#include <Common/Scheduler/ITimeSharedNode.h>
#include <Common/Scheduler/IWorkloadNode.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationLimit.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/FairAllocation.h>
#include <Common/Scheduler/Nodes/SpaceShared/PrecedenceAllocation.h>
#include <Common/Scheduler/Nodes/TimeShared/FairPolicy.h>
#include <Common/Scheduler/Nodes/TimeShared/FifoQueue.h>
#include <Common/Scheduler/Nodes/TimeShared/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/ThrottlerConstraint.h>
#include <Common/Scheduler/WorkloadSettings.h>

#include <memory>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
    extern const int LOGICAL_ERROR;
}

/// Traits to unify creation and management of scheduler nodes for both time-shared and space-shared resources
template <class BaseNode>
struct WorkloadNodeTraits;

template <>
struct WorkloadNodeTraits<ITimeSharedNode>
{
    using NodePtr = TimeSharedNodePtr;

    static NodePtr makeQueue(IWorkloadNode * workload, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit)
    {
        NodePtr result = std::make_shared<FifoQueue>(
            event_queue_,
            SchedulerNodeInfo{},
            settings_.getQueueLimit(unit));
        result->basename = "fifo";
        result->workload = workload;
        return result;
    }

    static ResourceLink getLink(const NodePtr & node)
    {
        return ResourceLink{.queue = &static_cast<FifoQueue &>(*node)};
    }

    static void updateQueue(const NodePtr & node, const WorkloadSettings & settings_, CostUnit unit)
    {
        static_cast<FifoQueue &>(*node).updateQueueLimit(settings_.getQueueLimit(unit));
    }

    static void purgeQueue(const NodePtr & node)
    {
        static_cast<FifoQueue &>(*node).purgeQueue();
    }

    static NodePtr makeFairPolicy(IWorkloadNode * workload, EventQueue & event_queue_, Priority priority)
    {
        NodePtr result = std::make_shared<FairPolicy>(event_queue_, SchedulerNodeInfo{});
        result->info.setPriority(priority);
        result->basename = fmt::format("p{}_fair", priority.value);
        result->workload = workload;
        return result;
    }

    static Priority getPriority(const SchedulerNodeInfo & info)
    {
        return info.priority; // time-shared nodes use priority directly
    }

    static NodePtr makePriorityPolicy(IWorkloadNode * workload, EventQueue & event_queue_)
    {
        NodePtr result = std::make_shared<PriorityPolicy>(event_queue_, SchedulerNodeInfo{});
        result->basename = "prio";
        result->workload = workload;
        return result;
    }

    static bool hasSemaphore(const WorkloadSettings & settings_, CostUnit unit)
    {
        return settings_.hasSemaphore(unit);
    }

    static NodePtr makeSemaphore(IWorkloadNode * workload, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit)
    {
        NodePtr result = std::make_shared<SemaphoreConstraint>(
            event_queue_,
            SchedulerNodeInfo{},
            settings_.getSemaphoreMaxRequests(unit),
            settings_.getSemaphoreMaxCost(unit));
        result->basename = "semaphore";
        result->workload = workload;
        return result;
    }

    static void updateSemaphore(const NodePtr & node, const WorkloadSettings & settings_, CostUnit unit)
    {
        static_cast<SemaphoreConstraint &>(*node).updateConstraints(
            node,
            settings_.getSemaphoreMaxRequests(unit),
            settings_.getSemaphoreMaxCost(unit));
    }

    static bool hasThrottler(const WorkloadSettings & settings_, CostUnit unit)
    {
        return settings_.hasThrottler(unit);
    }

    static NodePtr makeThrottler(IWorkloadNode * workload, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit)
    {
        NodePtr result = std::make_shared<ThrottlerConstraint>(
            event_queue_,
            SchedulerNodeInfo{},
            settings_.getThrottlerMaxSpeed(unit),
            settings_.getThrottlerMaxBurst(unit));
        result->basename = "throttler";
        result->workload = workload;
        return result;
    }

    static void updateThrottler(const NodePtr & node, const WorkloadSettings & settings_, CostUnit unit)
    {
        static_cast<ThrottlerConstraint &>(*node).updateConstraints(
            settings_.getThrottlerMaxSpeed(unit),
            settings_.getThrottlerMaxBurst(unit));
    }

    static constexpr bool addRawPointerThrottler = false; // ThrottlerConstraint does not call `request->addConstraint()`
    static constexpr bool addRawPointerSemaphore = true; // SemaphoreConstraint may be stored as a raw pointer in ResourceRequest
    static constexpr bool addRawPointerQueue = true; // ResourceLink holds raw pointer to FifoQueue - so we need to enforce destruction order here
};

template <>
struct WorkloadNodeTraits<ISpaceSharedNode>
{
    using NodePtr = SpaceSharedNodePtr;

    static NodePtr makeQueue(IWorkloadNode * workload, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit)
    {
        NodePtr result = std::make_shared<AllocationQueue>(
            event_queue_,
            SchedulerNodeInfo{},
            settings_.getQueueLimit(unit));
        result->basename = "queue";
        result->workload = workload;
        return result;
    }

    static ResourceLink getLink(const NodePtr & node)
    {
        return ResourceLink{.allocation_queue = &static_cast<AllocationQueue &>(*node)};
    }

    static void updateQueue(const NodePtr & node, const WorkloadSettings & settings_, CostUnit unit)
    {
        static_cast<AllocationQueue &>(*node).updateQueueLimit(settings_.getQueueLimit(unit));
    }

    static void purgeQueue(const NodePtr & node)
    {
        static_cast<AllocationQueue &>(*node).purgeQueue();
    }

    static NodePtr makeFairPolicy(IWorkloadNode * workload, EventQueue & event_queue_, Priority precedence)
    {
        NodePtr result = std::make_shared<FairAllocation>(event_queue_, SchedulerNodeInfo{});
        result->info.setPrecedence(precedence);
        result->basename = fmt::format("p{}_fair", precedence.value);
        result->workload = workload;
        return result;
    }

    static Priority getPriority(const SchedulerNodeInfo & info)
    {
        return info.precedence; // space-shared nodes use precedence for ordering instead of priority
    }

    static NodePtr makePriorityPolicy(IWorkloadNode * workload, EventQueue & event_queue_)
    {
        NodePtr result = std::make_shared<PrecedenceAllocation>(event_queue_, SchedulerNodeInfo{});
        result->basename = "prio";
        result->workload = workload;
        return result;
    }

    static bool hasSemaphore(const WorkloadSettings & settings_, CostUnit unit)
    {
        return settings_.hasAllocationLimit(unit);
    }

    static NodePtr makeSemaphore(IWorkloadNode * workload, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit)
    {
        NodePtr result = std::make_shared<AllocationLimit>(
            event_queue_,
            SchedulerNodeInfo{},
            settings_.getAllocationLimit(unit));
        result->basename = "limit";
        result->workload = workload;
        return result;
    }

    static void updateSemaphore(const NodePtr & node, const WorkloadSettings & settings_, CostUnit unit)
    {
        static_cast<AllocationLimit &>(*node).updateLimit(settings_.getAllocationLimit(unit));
    }

    static bool hasThrottler(const WorkloadSettings &, CostUnit)
    {
        return false;
    }

    static NodePtr makeThrottler(IWorkloadNode *, EventQueue &, const WorkloadSettings &, CostUnit)
    {
        chassert(false);
        return {};
    }

    static void updateThrottler(const NodePtr &, const WorkloadSettings &, CostUnit)
    {
        chassert(false);
    }

    static constexpr bool addRawPointerThrottler = false; // There is no throttler for space-shared resources
    static constexpr bool addRawPointerSemaphore = false; // AllocationLimit is never stored as a raw pointer in requests or allocations
    static constexpr bool addRawPointerQueue = true; // ResourceLink holds raw pointer to AllocationQueue - so we need to enforce destruction order here
};

template <class BaseNode, class Traits = WorkloadNodeTraits<BaseNode>>
class WorkloadNodeCommon : public BaseNode, public IWorkloadNode
{
public:
    using Base = BaseNode;
    using NodePtr = std::shared_ptr<BaseNode>;
    using SelfPtr = std::shared_ptr<WorkloadNodeCommon>;

protected:
    /// Helper function for managing a parent of a node
    static void reparent(const NodePtr & node, const NodePtr & new_parent)
    {
        reparent(node, new_parent.get());
    }

    /// Helper function for managing a parent of a node
    static void reparent(const NodePtr & node, ISchedulerNode * new_parent)
    {
        chassert(node);
        chassert(new_parent);
        if (new_parent == node->parent)
            return;
        if (node->parent)
            node->parent->removeChild(node.get());
        new_parent->attachChild(node);
    }

    /// Helper function for managing a parent of a node
    static void detach(const NodePtr & node)
    {
        if (node->parent)
            node->parent->removeChild(node.get());
    }

    /// A branch of the tree for a specific priority value
    struct FairnessBranch
    {
        NodePtr root; /// FairPolicy node is used if multiple children with the same priority are attached
        std::unordered_map<String, SelfPtr> children; // basename -> child

        bool empty() const { return children.empty(); }

        NodePtr getRoot()
        {
            chassert(!children.empty());
            if (root)
                return root;
            chassert(children.size() == 1);
            return children.begin()->second;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr attachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
                throw Exception(
                    ErrorCodes::INVALID_SCHEDULER_NODE,
                    "Can't add another child with the same path: {}",
                    it->second->getPath());

            if (children.size() == 2)
            {
                // Insert fair node if we have just added the second child
                chassert(!root);
                root = Traits::makeFairPolicy(self, event_queue_, Traits::getPriority(child->info));
                for (auto & [_, node] : children)
                    reparent(node, root);
                return root; // New root has been created
            }
            else if (children.size() == 1)
                return child; // We have added single child so far and it is the new root
            else
                reparent(child, root);
            return {}; // Root is the same
        }

        /// Detaches a child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        /// NOTE: It could also return null if `empty()` after detaching
        [[nodiscard]] NodePtr detachWorkloadChild(IWorkloadNode *, EventQueue &, const SelfPtr & child)
        {
            auto it = children.find(child->basename);
            if (it == children.end())
                return {}; // unknown child

            detach(child);
            children.erase(it);
            if (children.size() == 1)
            {
                // Remove fair if the only child has left
                chassert(root);
                auto remaining_child = children.begin()->second;
                detach(remaining_child); // Detach remaining child from root before destroying it
                detach(root);
                root.reset();
                return remaining_child; // The last child is a new root now
            }
            else if (children.empty())
                return {}; // We have detached the last child
            else
                return {}; // Root is the same (two or more children have left)
        }
    };

    /// Handles all the children nodes with intermediate fair and/or priority nodes
    struct ChildrenBranch
    {
        NodePtr root; /// PriorityPolicy node is used if multiple children with different priority are attached
        std::unordered_map<Priority::Value, FairnessBranch> branches; /// Branches for different priority values

        // Returns true iff there are no workload children attached
        bool empty() const { return branches.empty(); }

        NodePtr getRoot()
        {
            chassert(!branches.empty());
            if (root)
                return root;
            return branches.begin()->second.getRoot(); // There should be exactly one child-branch
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr attachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            auto [it, new_branch]  = branches.try_emplace(Traits::getPriority(child->info));
            auto & child_branch = it->second;
            auto branch_root = child_branch.attachWorkloadChild(self, event_queue_, child);
            if (!new_branch)
            {
                if (branch_root)
                {
                    if (root)
                        reparent(branch_root, root);
                    else
                        return branch_root;
                }
                return {};
            }
            else
            {
                chassert(branch_root);
                if (branches.size() == 2)
                {
                    // Insert priority node if we have just added the second branch
                    chassert(!root);
                    root = Traits::makePriorityPolicy(self, event_queue_);
                    for (auto & [_, branch] : branches)
                        reparent(branch.getRoot(), root);
                    return root; // New root has been created
                }
                else if (branches.size() == 1)
                    return child; // We have added single child so far and it is the new root
                else
                    reparent(child, root);
                return {}; // Root is the same
            }
        }

        /// Detaches a child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        /// NOTE: It could also return null if `empty()` after detaching
        [[nodiscard]] NodePtr detachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            auto it = branches.find(Traits::getPriority(child->info));
            if (it == branches.end())
                return {}; // unknown child

            auto & child_branch = it->second;
            auto branch_root = child_branch.detachWorkloadChild(self, event_queue_, child);
            if (child_branch.empty())
            {
                branches.erase(it);
                if (branches.size() == 1)
                {
                    // Remove priority node if the only child-branch has left
                    chassert(root);
                    auto remaining_branch_root = branches.begin()->second.getRoot();
                    detach(remaining_branch_root); // Detach remaining branch from root before destroying it
                    detach(root);
                    root.reset();
                    return remaining_branch_root; // The last child-branch is a new root now
                }
                else if (branches.empty())
                    return {}; // We have detached the last child
                else
                    return {}; // Root is the same (two or more children-branches have left)
            }
            if (branch_root)
            {
                if (root)
                    reparent(branch_root, root);
                else
                    return branch_root;
            }
            return {}; // Root is the same
        }
    };

    /// Handles degenerate case of zero children (a fifo queue) or delegate to `ChildrenBranch`.
    struct QueueOrChildrenBranch
    {
        NodePtr queue; /// FifoQueue or AllocationQueue node is used if there are no children
        ChildrenBranch branch; /// Used if there is at least one child
        WorkloadSettings settings;
        CostUnit unit;

        NodePtr getRoot()
        {
            if (queue)
                return queue;
            else
                return branch.getRoot();
        }

        // Should be called after constructor, before any other methods
        [[nodiscard]] NodePtr initialize(IWorkloadNode * self, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit_)
        {
            settings = settings_;
            unit = unit_;
            queue = Traits::makeQueue(self, event_queue_, settings, unit);
            return queue;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr attachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            if (queue)
                removeQueue();
            return branch.attachWorkloadChild(self, event_queue_, child);
        }

        /// Detaches a child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr detachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            if (queue)
                return {}; // No-op, it already has no children
            auto branch_root = branch.detachWorkloadChild(self, event_queue_, child);
            if (branch.empty())
            {
                queue = Traits::makeQueue(self, event_queue_, settings, unit);
                return queue;
            }
            return branch_root;
        }

        void updateSchedulingSettings(IWorkloadNode * self, EventQueue & event_queue_, const WorkloadSettings & new_settings)
        {
            UNUSED(self, event_queue_);
            settings = new_settings;
            if (queue)
                Traits::updateQueue(queue, settings, unit);
        }

    private:
        void removeQueue()
        {
            // This workload node will not be able to process resource requests any longer
            // All remaining resource requests are be aborted on queue destruction
            detach(queue);
            Traits::purgeQueue(queue);
            queue.reset();
        }
    };

    /// Handles all the nodes under this workload node
    /// Specifically handles constraints with `QueueOrChildrenBranch` under it
    struct ConstraintsBranch
    {
        NodePtr throttler;
        NodePtr semaphore;
        QueueOrChildrenBranch branch;
        WorkloadSettings settings;
        CostUnit unit;
        String resource;

        // Should be called after constructor, before any other methods
        [[nodiscard]] NodePtr initialize(IWorkloadNode * self, EventQueue & event_queue_, const WorkloadSettings & settings_, CostUnit unit_, const String & resource_)
        {
            settings = settings_;
            unit = unit_;
            resource = resource_;
            NodePtr node = branch.initialize(self, event_queue_, settings, unit);
            if (Traits::hasSemaphore(settings, unit))
            {
                semaphore = Traits::makeSemaphore(self, event_queue_, settings, unit);
                reparent(node, semaphore);
                node = semaphore;
            }
            if (Traits::hasThrottler(settings, unit))
            {
                throttler = Traits::makeThrottler(self, event_queue_, settings, unit);
                reparent(node, throttler);
                node = throttler;
            }
            return node;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr attachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            if (auto branch_root = branch.attachWorkloadChild(self, event_queue_, child))
            {
                // If both semaphore and throttler exist we should reparent to the farthest from the root
                if (semaphore)
                    reparent(branch_root, semaphore);
                else if (throttler)
                    reparent(branch_root, throttler);
                else
                    return branch_root;
            }
            return {};
        }

        /// Detaches a child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr detachWorkloadChild(IWorkloadNode * self, EventQueue & event_queue_, const SelfPtr & child)
        {
            if (auto branch_root = branch.detachWorkloadChild(self, event_queue_, child))
            {
                if (semaphore)
                    reparent(branch_root, semaphore);
                else if (throttler)
                    reparent(branch_root, throttler);
                else
                    return branch_root;
            }
            return {};
        }

        /// Updates constraint-related nodes.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] NodePtr updateSchedulingSettings(IWorkloadNode * self, EventQueue & event_queue_, const WorkloadSettings & new_settings)
        {
            branch.updateSchedulingSettings(self, event_queue_, new_settings);

            NodePtr node = branch.getRoot();

            if (!Traits::hasSemaphore(settings, unit) && Traits::hasSemaphore(new_settings, unit)) // Add
            {
                semaphore = Traits::makeSemaphore(self, event_queue_, new_settings, unit);
                reparent(node, semaphore);
                node = semaphore;
            }
            else if (Traits::hasSemaphore(settings, unit) && !Traits::hasSemaphore(new_settings, unit)) // Remove
            {
                detach(semaphore);
                semaphore.reset();
            }
            else if (Traits::hasSemaphore(settings, unit) && Traits::hasSemaphore(new_settings, unit)) // Update
            {
                Traits::updateSemaphore(semaphore, new_settings, unit);
                node = semaphore;
            }

            if (!Traits::hasThrottler(settings, unit) && Traits::hasThrottler(new_settings, unit)) // Add
            {
                throttler = Traits::makeThrottler(self, event_queue_, new_settings, unit);
                reparent(node, throttler);
                node = throttler;
            }
            else if (Traits::hasThrottler(settings, unit) && !Traits::hasThrottler(new_settings, unit)) // Remove
            {
                detach(throttler);
                throttler.reset();
            }
            else if (Traits::hasThrottler(settings, unit) && Traits::hasThrottler(new_settings, unit)) // Update
            {
                Traits::updateThrottler(throttler, new_settings, unit);
                node = throttler;
            }

            settings = new_settings;
            return node;
        }
    };

public:
    WorkloadNodeCommon(EventQueue & event_queue_, const WorkloadSettings & settings)
        : BaseNode(event_queue_, SchedulerNodeInfo(settings))
    {}

    void attachWorkloadChild(const WorkloadNodePtr & child) final
    {
        if (auto new_child = impl.attachWorkloadChild(this, this->event_queue, std::static_pointer_cast<WorkloadNodeCommon>(child)))
            reparent(new_child, this);
    }

    void detachWorkloadChild(const WorkloadNodePtr & child) final
    {
        if (auto new_child = impl.detachWorkloadChild(this, this->event_queue, std::static_pointer_cast<WorkloadNodeCommon>(child)))
            reparent(new_child, this);
    }

    void updateSchedulingSettings(const WorkloadSettings & new_settings) final
    {
        this->info.update(new_settings);
        propagateUpdateSchedulingSettings();
        if (auto new_child = impl.updateSchedulingSettings(this, this->event_queue, new_settings))
            reparent(new_child, this);
    }

    /// Sharing-mode specific implementation of scheduling settings propagation
    virtual void propagateUpdateSchedulingSettings() = 0;

    CostUnit getCostUnit() const final
    {
        return impl.unit;
    }

    const String & getWorkload() const final
    {
        return this->basename;
    }

    const String & getResource() const final
    {
        return impl.resource;
    }

    const WorkloadSettings & getSettings() const final
    {
        return impl.settings;
    }

    ResourceLink getLink() const final
    {
        if (impl.branch.queue)
            return Traits::getLink(impl.branch.queue);
        else
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Unable to link to workload '{}' that has children", this->basename);
    }

    void addRawPointerNodes(std::vector<SchedulerNodePtr> & nodes) final
    {
        if constexpr (Traits::addRawPointerThrottler)
        {
            if (impl.throttler)
                nodes.push_back(impl.throttler);
        }
        if constexpr (Traits::addRawPointerSemaphore)
        {
            if (impl.semaphore)
                nodes.push_back(impl.semaphore);
        }
        if constexpr (Traits::addRawPointerQueue)
        {
            if (impl.branch.queue)
                nodes.push_back(impl.branch.queue);
        }
        for (auto & [_0, branch] : impl.branch.branch.branches)
        {
            for (auto & [_1, child] : branch.children)
                child->addRawPointerNodes(nodes);
        }
    }

    bool hasWorkloadChildren() const final
    {
        return impl.branch.queue == nullptr;
    }

    void forEachSchedulerNode(std::function<void(ISchedulerNode *)> visitor) final
    {
        visitor(this);
        if (impl.throttler)
            visitor(impl.throttler.get());
        if (impl.semaphore)
            visitor(impl.semaphore.get());
        if (impl.branch.queue)
            visitor(impl.branch.queue.get());
        if (impl.branch.branch.root) // priority
            visitor(impl.branch.branch.root.get());
        for (auto & [_, branch] : impl.branch.branch.branches)
        {
            if (branch.root) // fairness
                visitor(branch.root.get());
        }
    }

protected: // Hide all the ISchedulerNode interface methods as an implementation details
    const String & getTypeName() const override
    {
        static String type_name("workload");
        return type_name;
    }

    bool equals(ISchedulerNode *) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WorkloadNode should not be used with CustomResourceManager");
    }

    ConstraintsBranch impl;
};

class TimeSharedWorkloadNode final : public WorkloadNodeCommon<ITimeSharedNode>
{
public:
    TimeSharedWorkloadNode(EventQueue & event_queue_, const WorkloadSettings & settings, CostUnit unit, const String & resource)
        : WorkloadNodeCommon(event_queue_, settings)
    {
        child = impl.initialize(this, event_queue_, settings, unit, resource);
        reparent(child, this);
    }

    ~TimeSharedWorkloadNode() override
    {
        if (child)
            this->removeChild(child.get());
    }

private:
    /// Attaches an immediate child (used through `reparent()`)
    void attachChild(const SchedulerNodePtr & child_) override
    {
        child = std::static_pointer_cast<ITimeSharedNode>(child_);
        child->setParentNode(this);

        // Activate if required
        if (child->isActive())
            activateChild(*child);
    }

    /// Removes an immediate child (used through `reparent()`)
    void removeChild(ISchedulerNode * child_) override
    {
        if (child.get() == child_)
        {
            child_active = false; // deactivate
            child->setParentNode(nullptr); // detach
            child.reset();
        }
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (child->basename == child_name)
            return child.get();
        else
            return nullptr;
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        // Dequeue request from the child
        auto [request, child_now_active] = child->dequeueRequest();

        // Deactivate if necessary
        child_active = child_now_active;
        if (!child_active)
            this->busy_periods++;

        if (request)
            incrementDequeued(request->cost);

        return {request, child_active};
    }

    bool isActive() override
    {
        return child_active;
    }

    /// Shows number of immediate active children (for introspection)
    size_t activeChildren() override
    {
        return child_active;
    }

    /// Activate an immediate child for a specific kind of resource request
    void activateChild(ITimeSharedNode & child_) override
    {
        if (&child_ == child.get())
            if (!std::exchange(child_active, true) && this->parent)
                castParent().activateChild(*this);
    }

    void propagateUpdateSchedulingSettings() override
    {
        // No-op for time-shared workloads.
        // Weights and priorities are used by dequeueRequest() directly and are not stored.
    }

    TimeSharedNodePtr child; // An immediate child (actually the root of the whole subtree)
    bool child_active = false;
};

class SpaceSharedWorkloadNode final : public WorkloadNodeCommon<ISpaceSharedNode>
{
public:
    SpaceSharedWorkloadNode(EventQueue & event_queue_, const WorkloadSettings & settings, CostUnit unit, const String & resource)
        : WorkloadNodeCommon(event_queue_, settings)
    {
        child = impl.initialize(this, event_queue_, settings, unit, resource);
        reparent(child, this);
    }

    ~SpaceSharedWorkloadNode() override
    {
        if (child)
            this->removeChild(child.get());
    }

private:
    /// Attaches an immediate child (used through `reparent()`)
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override
    {
        child = std::static_pointer_cast<ISpaceSharedNode>(child_);
        child->setParentNode(this);
        propagateUpdate(*child, Update()
            .setAttached(child.get())
            .setIncrease(child->increase)
            .setDecrease(child->decrease));
    }

    /// Removes an immediate child (used through `reparent()`)
    void removeChild(ISchedulerNode * child_) override
    {
        if (child.get() != child_)
            return;
        propagateUpdate(*child, Update()
            .setDetached(child.get())
            .setIncrease(nullptr)
            .setDecrease(nullptr));
        child->setParentNode(nullptr);
        child.reset();
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (child->basename == child_name)
            return child.get();
        else
            return nullptr;
    }

    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override
    {
        chassert(&from_child == child.get());
        apply(update);
        if (update.increase)
            increase = *update.increase;
        if (update.decrease)
            decrease = *update.decrease;
        if (parent && update)
            propagate(std::move(update));
    }

    void approveIncrease() override
    {
        chassert(increase);
        apply(*increase);
        increase = nullptr;
        child->approveIncrease();
        increase = child->increase;
    }

    void approveDecrease() override
    {
        chassert(decrease);
        apply(*decrease);
        decrease = nullptr;
        child->approveDecrease();
        decrease = child->decrease;
    }

    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override
    {
        chassert(child);
        return child->selectAllocationToKill(killer, limit, details);
    }

    void propagateUpdateSchedulingSettings() override
    {
        if (parent)
        {
            // If parent is FairAllocation we need to propagate the weight change to update the key
            propagate(Update()
                .setIncrease(increase)
                .setDecrease(decrease));
        }
    }

    SpaceSharedNodePtr child; // An immediate child (actually the root of the whole subtree)
};

using TimeSharedWorkloadNodePtr = TimeSharedWorkloadNode::SelfPtr;
using SpaceSharedWorkloadNodePtr = SpaceSharedWorkloadNode::SelfPtr;

}
