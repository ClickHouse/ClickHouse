#pragma once

#include <Common/Priority.h>
#include <Common/Scheduler/Nodes/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/FairPolicy.h>
#include <Common/Scheduler/Nodes/ThrottlerConstraint.h>
#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>
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

/*
 * Unified scheduler node combines multiple nodes internally to provide all available scheduling policies and constraints.
 * Whole scheduling hierarchy could "logically" consist of unified nodes only. Physically intermediate "internal" nodes
 * are also present. This approch is easiers for manipulations in runtime than using multiple types of nodes.
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
class UnifiedSchedulerNode : public ISchedulerNode
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
        chassert(new_parent);
        if (new_parent == node->parent)
            return;
        if (node->parent)
            node->parent->removeChild(node.get());
        new_parent->attachChild(node);
    }

    /// A branch of the tree for a specific priority value
    struct FairnessBranch {
        SchedulerNodePtr root; /// FairPolicy node is used if multiple children with the same priority are attached
        std::unordered_map<String, SchedulerNodePtr> children; // basename -> child

        SchedulerNodePtr getRoot()
        {
            chassert(!children.empty());
            if (root)
                return root;
            return children.begin()->second; // There should be exactly one child
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const SchedulerNodePtr & child)
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
    };

    /// Handles all the children nodes with intermediate fair and/or priority nodes
    struct ChildrenBranch
    {
        SchedulerNodePtr root; /// PriorityPolicy node is used if multiple children with different priority are attached
        std::unordered_map<Priority::Value, FairnessBranch> branches; /// Branches for different priority values

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const SchedulerNodePtr & child)
        {
            bool existing_branch = branches.contains(child->info.priority);
            auto & child_branch = branches[child->info.priority];
            auto branch_root = child_branch.attachUnifiedChild(event_queue_, child);

            if (existing_branch)
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
    };

    /// Handles degenerate case of zero children (a fifo queue) or delegate to `ChildrenBranch`.
    struct QueueOrChildrenBranch
    {
        SchedulerNodePtr queue; /// FifoQueue node is used if there are no children
        ChildrenBranch branch; /// Used if there is at least one child

        // Should be called after constructor, before any other methods
        [[nodiscard]] SchedulerNodePtr initialize(EventQueue * event_queue_)
        {
            createQueue(event_queue_);
            return queue;
        }

        /// Attaches a new child.
        /// Returns root node if it has been changed to a different node, otherwise returns null.
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const SchedulerNodePtr & child)
        {
            if (queue)
                removeQueue(event_queue_);
            return branch.attachUnifiedChild(event_queue_, child);
        }

    private:
        void createQueue(EventQueue * event_queue_)
        {
            queue = std::make_shared<FifoQueue>(event_queue_, SchedulerNodeInfo{});
            queue->basename = "fifo";
        }

        void removeQueue(EventQueue *)
        {
            // This unified node will not be able to process resource requests any longer
            // All remaining resource requests are be aborted on queue destruction
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
        [[nodiscard]] SchedulerNodePtr attachUnifiedChild(EventQueue * event_queue_, const SchedulerNodePtr & child)
        {
            if (auto branch_root = branch.attachUnifiedChild(event_queue_, child))
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
    };

public:
    explicit UnifiedSchedulerNode(EventQueue * event_queue_, const SchedulingSettings & settings)
        : ISchedulerNode(event_queue_, SchedulerNodeInfo(settings.weight, settings.priority))
    {
        immediate_child = impl.initialize(event_queue, settings);
        reparent(immediate_child, this);
    }

    /// Attaches a child as a leaf of internal subtree and insert or update all the intermediate nodes
    /// NOTE: Do not confuse with `attachChild()` which is used only for immediate children
    void attachUnifiedChild(const SchedulerNodePtr & child)
    {
        if (auto new_child = impl.attachUnifiedChild(event_queue, child))
            reparent(new_child, this);
    }

    /// Updates intermediate nodes subtree according with new priority (priority is set by the caller beforehand)
    /// NOTE: Changing a priority of a unified child may lead to change of its parent.
    void updateUnifiedChildPriority(const SchedulerNodePtr & child, Priority old_priority, Priority new_priority)
    {
        UNUSED(child, old_priority, new_priority); // TODO: implement updateUnifiedChildPriority
    }

    /// Updates scheduling settings. Set of constraints might change.
    /// NOTE: Caller is responsible for calling `updateUnifiedChildPriority` in parent unified node (if any)
    void updateSchedulingSettings(const SchedulingSettings & new_settings)
    {
        UNUSED(new_settings); // TODO: implement updateSchedulingSettings
    }

    /// Returns the queue to be used for resource requests or `nullptr` if it has unified children
    ISchedulerQueue * getQueue()
    {
        return static_cast<ISchedulerQueue *>(impl.branch.queue.get());
    }

protected: // Hide all the ISchedulerNode interface methods as an implementation details
    bool equals(ISchedulerNode *) override
    {
        assert(false);
        return false;
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

using UnifiedSchedulerNodePtr = std::shared_ptr<UnifiedSchedulerNode>;

}
