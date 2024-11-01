#pragma once

#include <Common/Priority.h>
#include <Common/Scheduler/Nodes/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/FairPolicy.h>
#include <Common/Scheduler/Nodes/ThrottlerConstraint.h>
#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/Nodes/FifoQueue.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/SchedulingSettings.h>
#include <Common/Exception.h>

#include <memory>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
    extern const int LOGICAL_ERROR;
}

class UnifiedSchedulerNode;
using UnifiedSchedulerNodePtr = std::shared_ptr<UnifiedSchedulerNode>;

/*
 * Unified scheduler node combines multiple nodes internally to provide all available scheduling policies and constraints.
 * Whole scheduling hierarchy could "logically" consist of unified nodes only. Physically intermediate "internal" nodes
 * are also present. This approach is easiers for manipulations in runtime than using multiple types of nodes.
 *
 * Unified node is capable of updating its internal structure based on:
 * 1. Number of children (fifo if =0 or fairness/priority if >0).
 * 2. Priorities of its children (for subtree structure).
 * 3. `SchedulingSettings` associated with unified node (for throttler and semaphore constraints).
 *
 * In general, unified node has "internal" subtree with the following structure:
 *
 *                            THIS           <-- UnifiedSchedulerNode object
 *                              |
 *                          THROTTLER        <-- [Optional] Throttling scheduling constraint
 *                              |
 *   [If no children]------ SEMAPHORE        <-- [Optional] Semaphore constraint
 *           |                  |
 *         FIFO             PRIORITY         <-- [Optional] Scheduling policy distinguishing priorities
 *                 .-------'        '-------.
 *       FAIRNESS[p1]          ...         FAIRNESS[pN] <-- [Optional] Policies for fairness if priorities are equal
 *        /        \                        /        \
 *  CHILD[p1,w1] ... CHILD[p1,wM]  CHILD[pN,w1] ... CHILD[pN,wM]  <-- Unified children (UnifiedSchedulerNode objects)
 *
 * NOTE: to distinguish different kinds of children we use the following terms:
 *  - immediate child: child of unified object (THROTTLER);
 *  - unified child: leaf of this "internal" subtree (CHILD[p,w]);
 *  - intermediate node: any child that is not UnifiedSchedulerNode (unified child or `this`)
 */
class UnifiedSchedulerNode final : public ISchedulerNode
{
private:
    /// Helper function for managing a parent of a node
    static void reparent(const SchedulerNodePtr & node, const SchedulerNodePtr & new_parent)
    {
        reparent(node, new_parent.get());
    }

    /// Helper function for managing a parent of a node
    static void reparent(const SchedulerNodePtr & node, ISchedulerNode * new_parent)
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
    static void detach(const SchedulerNodePtr & node)
    {
        if (node->parent)
            node->parent->removeChild(node.get());
    }

    /// A branch of the tree for a specific priority value
    struct FairnessBranch
    {
        SchedulerNodePtr root; /// FairPolicy node is used if multiple children with the same priority are attached
        std::unordered_map<String, UnifiedSchedulerNodePtr> children; // basename -> child

        bool empty() const { return children.empty(); }

        SchedulerNodePtr getRoot()
        {
            chassert(!children.empty());
            if (root)
                return root;
            chassert(children.size() == 1);
            return children.begin()->second;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
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
                root = std::make_shared<FairPolicy>(event_queue_, SchedulerNodeInfo{});
                root->info.setPriority(child->info.priority);
                root->basename = fmt::format("p{}_fair", child->info.priority.value);
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
        [[nodiscard]] SchedulerNodePtr detachUnifiedChild(EventQueue *, const UnifiedSchedulerNodePtr & child)
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
                detach(root);
                root.reset();
                return children.begin()->second; // The last child is a new root now
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
        SchedulerNodePtr root; /// PriorityPolicy node is used if multiple children with different priority are attached
        std::unordered_map<Priority::Value, FairnessBranch> branches; /// Branches for different priority values

        // Returns true iff there are no unified children attached
        bool empty() const { return branches.empty(); }

        SchedulerNodePtr getRoot()
        {
            chassert(!branches.empty());
            if (root)
                return root;
            return branches.begin()->second.getRoot(); // There should be exactly one child-branch
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
        {
            auto [it, new_branch]  = branches.try_emplace(child->info.priority);
            auto & child_branch = it->second;
            auto branch_root = child_branch.attachUnifiedChild(event_queue_, child);
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
                    root = std::make_shared<PriorityPolicy>(event_queue_, SchedulerNodeInfo{});
                    root->basename = "prio";
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
        [[nodiscard]] SchedulerNodePtr detachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
        {
            auto it = branches.find(child->info.priority);
            if (it == branches.end())
                return {}; // unknown child

            auto & child_branch = it->second;
            auto branch_root = child_branch.detachUnifiedChild(event_queue_, child);
            if (child_branch.empty())
            {
                branches.erase(it);
                if (branches.size() == 1)
                {
                    // Remove priority node if the only child-branch has left
                    chassert(root);
                    detach(root);
                    root.reset();
                    return branches.begin()->second.getRoot(); // The last child-branch is a new root now
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
        SchedulerNodePtr queue; /// FifoQueue node is used if there are no children
        ChildrenBranch branch; /// Used if there is at least one child

        SchedulerNodePtr getRoot()
        {
            if (queue)
                return queue;
            else
                return branch.getRoot();
        }

        // Should be called after constructor, before any other methods
        [[nodiscard]] SchedulerNodePtr initialize(EventQueue * event_queue_)
        {
            createQueue(event_queue_);
            return queue;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
        {
            if (queue)
                removeQueue();
            return branch.attachUnifiedChild(event_queue_, child);
        }

        /// Detaches a child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr detachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
        {
            if (queue)
                return {}; // No-op, it already has no children
            auto branch_root = branch.detachUnifiedChild(event_queue_, child);
            if (branch.empty())
            {
                createQueue(event_queue_);
                return queue;
            }
            return branch_root;
        }

    private:
        void createQueue(EventQueue * event_queue_)
        {
            queue = std::make_shared<FifoQueue>(event_queue_, SchedulerNodeInfo{});
            queue->basename = "fifo";
        }

        void removeQueue()
        {
            // This unified node will not be able to process resource requests any longer
            // All remaining resource requests are be aborted on queue destruction
            detach(queue);
            std::static_pointer_cast<ISchedulerQueue>(queue)->purgeQueue();
            queue.reset();
        }
    };

    /// Handles all the nodes under this unified node
    /// Specifically handles constraints with `QueueOrChildrenBranch` under it
    struct ConstraintsBranch
    {
        SchedulerNodePtr throttler;
        SchedulerNodePtr semaphore;
        QueueOrChildrenBranch branch;
        SchedulingSettings settings;

        // Should be called after constructor, before any other methods
        [[nodiscard]] SchedulerNodePtr initialize(EventQueue * event_queue_, const SchedulingSettings & settings_)
        {
            settings = settings_;
            SchedulerNodePtr node = branch.initialize(event_queue_);
            if (settings.hasSemaphore())
            {
                semaphore = std::make_shared<SemaphoreConstraint>(event_queue_, SchedulerNodeInfo{}, settings.max_requests, settings.max_cost);
                semaphore->basename = "semaphore";
                reparent(node, semaphore);
                node = semaphore;
            }
            if (settings.hasThrottler())
            {
                throttler = std::make_shared<ThrottlerConstraint>(event_queue_, SchedulerNodeInfo{}, settings.max_speed, settings.max_burst);
                throttler->basename = "throttler";
                reparent(node, throttler);
                node = throttler;
            }
            return node;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
        {
            if (auto branch_root = branch.attachUnifiedChild(event_queue_, child))
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
        [[nodiscard]] SchedulerNodePtr detachUnifiedChild(EventQueue * event_queue_, const UnifiedSchedulerNodePtr & child)
        {
            if (auto branch_root = branch.detachUnifiedChild(event_queue_, child))
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
        [[nodiscard]] SchedulerNodePtr updateSchedulingSettings(EventQueue * event_queue_, const SchedulingSettings & new_settings)
        {
            SchedulerNodePtr node = branch.getRoot();

            if (!settings.hasSemaphore() && new_settings.hasSemaphore()) // Add semaphore
            {
                semaphore = std::make_shared<SemaphoreConstraint>(event_queue_, SchedulerNodeInfo{}, new_settings.max_requests, new_settings.max_cost);
                semaphore->basename = "semaphore";
                reparent(node, semaphore);
                node = semaphore;
            }
            else if (settings.hasSemaphore() && !new_settings.hasSemaphore()) // Remove semaphore
            {
                detach(semaphore);
                semaphore.reset();
            }
            else if (settings.hasSemaphore() && new_settings.hasSemaphore()) // Update semaphore
            {
                static_cast<SemaphoreConstraint&>(*semaphore).updateConstraints(semaphore, new_settings.max_requests, new_settings.max_cost);
                node = semaphore;
            }

            if (!settings.hasThrottler() && new_settings.hasThrottler()) // Add throttler
            {
                throttler = std::make_shared<ThrottlerConstraint>(event_queue_, SchedulerNodeInfo{}, new_settings.max_speed, new_settings.max_burst);
                throttler->basename = "throttler";
                reparent(node, throttler);
                node = throttler;
            }
            else if (settings.hasThrottler() && !new_settings.hasThrottler()) // Remove throttler
            {
                detach(throttler);
                throttler.reset();
            }
            else if (settings.hasThrottler() && new_settings.hasThrottler()) // Update throttler
            {
                static_cast<ThrottlerConstraint&>(*throttler).updateConstraints(new_settings.max_speed, new_settings.max_burst);
                node = throttler;
            }

            settings = new_settings;
            return node;
        }
    };

public:
    explicit UnifiedSchedulerNode(EventQueue * event_queue_, const SchedulingSettings & settings)
        : ISchedulerNode(event_queue_, SchedulerNodeInfo(settings.weight, settings.priority))
    {
        immediate_child = impl.initialize(event_queue, settings);
        reparent(immediate_child, this);
    }

    ~UnifiedSchedulerNode() override
    {
        // We need to clear `parent` in child to avoid dangling references
        if (immediate_child)
            removeChild(immediate_child.get());
    }

    /// Attaches a unified child as a leaf of internal subtree and insert or update all the intermediate nodes
    /// NOTE: Do not confuse with `attachChild()` which is used only for immediate children
    void attachUnifiedChild(const UnifiedSchedulerNodePtr & child)
    {
        if (auto new_child = impl.attachUnifiedChild(event_queue, child))
            reparent(new_child, this);
    }

    /// Detaches unified child and update all the intermediate nodes.
    /// Detached child could be safely attached to another parent.
    /// NOTE: Do not confuse with `removeChild()` which is used only for immediate children
    void detachUnifiedChild(const UnifiedSchedulerNodePtr & child)
    {
        if (auto new_child = impl.detachUnifiedChild(event_queue, child))
            reparent(new_child, this);
    }

    static bool updateRequiresDetach(const String & old_parent, const String & new_parent, const SchedulingSettings & old_settings, const SchedulingSettings & new_settings)
    {
        return old_parent != new_parent || old_settings.priority != new_settings.priority;
    }

    /// Updates scheduling settings. Set of constraints might change.
    /// NOTE: Caller is responsible for detaching and attaching if `updateRequiresDetach` returns true
    void updateSchedulingSettings(const SchedulingSettings & new_settings)
    {
        info.setPriority(new_settings.priority);
        info.setWeight(new_settings.weight);
        if (auto new_child = impl.updateSchedulingSettings(event_queue, new_settings))
            reparent(new_child, this);
    }

    const SchedulingSettings & getSettings() const
    {
        return impl.settings;
    }

    /// Returns the queue to be used for resource requests or `nullptr` if it has unified children
    std::shared_ptr<ISchedulerQueue> getQueue() const
    {
        return static_pointer_cast<ISchedulerQueue>(impl.branch.queue);
    }

    /// Collects nodes that could be accessed with raw pointers by resource requests (queue and constraints)
    /// NOTE: This is a building block for classifier. Note that due to possible movement of a queue, set of constraints
    /// for that queue might change in future, and `request->constraints` might reference nodes not in
    /// the initial set of nodes returned by `addRawPointerNodes()`. To avoid destruction of such additional nodes
    /// classifier must (indirectly) hold nodes return by `addRawPointerNodes()` for all future versions of
    /// all unified nodes. Such a version control is done by `IOResourceManager`.
    void addRawPointerNodes(std::vector<SchedulerNodePtr> & nodes)
    {
        // NOTE: `impl.throttler` could be skipped, because ThrottlerConstraint does not call `request->addConstraint()`
        if (impl.semaphore)
            nodes.push_back(impl.semaphore);
        if (impl.branch.queue)
            nodes.push_back(impl.branch.queue);
        for (auto & [_0, branch] : impl.branch.branch.branches)
        {
            for (auto & [_1, child] : branch.children)
                child->addRawPointerNodes(nodes);
        }
    }

    bool hasUnifiedChildren() const
    {
        return impl.branch.queue == nullptr;
    }

    /// Introspection. Calls a visitor for self and every internal node. Do not recurse into unified children.
    void forEachSchedulerNode(std::function<void(ISchedulerNode *)> visitor)
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
        static String type_name("unified");
        return type_name;
    }

    bool equals(ISchedulerNode *) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UnifiedSchedulerNode should not be used with CustomResourceManager");
    }

    /// Attaches an immediate child (used through `reparent()`)
    void attachChild(const SchedulerNodePtr & child_) override
    {
        immediate_child = child_;
        immediate_child->setParent(this);

        // Activate if required
        if (immediate_child->isActive())
            activateChild(immediate_child.get());
    }

    /// Removes an immediate child (used through `reparent()`)
    void removeChild(ISchedulerNode * child) override
    {
        if (immediate_child.get() == child)
        {
            child_active = false; // deactivate
            immediate_child->setParent(nullptr); // detach
            immediate_child.reset();
        }
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (immediate_child->basename == child_name)
            return immediate_child.get();
        else
            return nullptr;
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        auto [request, child_now_active] = immediate_child->dequeueRequest();
        if (!request)
            return {nullptr, false};

        child_active = child_now_active;
        if (!child_active)
            busy_periods++;
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

    /// Activate an immediate child
    void activateChild(ISchedulerNode * child) override
    {
        if (child == immediate_child.get())
            if (!std::exchange(child_active, true) && parent)
                parent->activateChild(this);
    }

private:
    ConstraintsBranch impl;
    SchedulerNodePtr immediate_child; // An immediate child (actually the root of the whole subtree)
    bool child_active = false;
};

}
