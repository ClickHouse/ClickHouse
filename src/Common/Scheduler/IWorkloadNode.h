#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/CostUnit.h>

#include <memory>


namespace DB
{

class IWorkloadNode;
using WorkloadNodePtr = std::shared_ptr<IWorkloadNode>;

/// Workload node combines multiple scheduler nodes internally to provide all available scheduling policies and constraints.
/// Whole scheduling hierarchy could "logically" consist of workload nodes only. Physically intermediate "internal" nodes
/// are also present. This approach is easiers for manipulations in runtime than using multiple types of nodes.
///
/// There are two implementation: for time-shared and space-shared resources.
///
/// Workload node is capable of updating its internal structure based on:
/// 1. Number of children (fifo if =0 or fairness/priority/precedence if >0).
/// 2. Priorities of its children (for subtree structure).
/// 3. `WorkloadSettings` associated with workload node (for throttler and semaphore constraints).
///
/// In general, workload node has "internal" subtree with the following structure:
///
///                            THIS           <-- WorkloadNode object
///                              |
///                          THROTTLER        <-- [Optional] Throttling scheduling constraint
///                              |
///   [If no children]------ SEMAPHORE        <-- [Optional] Semaphore constraint (or limit for space-shared resource)
///           |                  |
///         FIFO             PRIORITY         <-- [Optional] Scheduling policy distinguishing priorities or precedence
///                 .-------'        '-------.
///       FAIRNESS[p1]          ...         FAIRNESS[pN] <-- [Optional] Policies for fairness if priorities are equal
///        /        \                        /        \
///  CHILD[p1,w1] ... CHILD[p1,wM]  CHILD[pN,w1] ... CHILD[pN,wM]  <-- Workload children (WorkloadNode objects)
///
/// NOTE: to distinguish different kinds of children we use the following terms:
///  - immediate child: child of workload object (e.g. THROTTLER);
///  - workload child: leaf of this "internal" subtree (CHILD[p,w]);
///  - intermediate node: any child that is not WorkloadNode (workload child or `this`)
class IWorkloadNode
{
public:
    virtual ~IWorkloadNode() = default;

    /// Attaches a workload child as a leaf of internal subtree and insert or update all the intermediate nodes
    /// NOTE: Do not confuse with `attachChild()` which is used only for immediate children
    virtual void attachWorkloadChild(const WorkloadNodePtr & child) = 0;

    /// Detaches workload child and update all the intermediate nodes.
    /// Detached child could be safely attached to another parent.
    /// NOTE: Do not confuse with `removeChild()` which is used only for immediate children
    virtual void detachWorkloadChild(const WorkloadNodePtr & child) = 0;

    /// Updates scheduling settings. Set of constraints might change.
    /// NOTE: Caller is responsible for detaching and attaching if `updateRequiresDetach` returns true
    virtual void updateSchedulingSettings(const WorkloadSettings & new_settings) = 0;

    /// Helper function to determine if detaching/attaching is required for specific update of workload node
    static bool updateRequiresDetach(
        const String & old_parent,
        const String & new_parent,
        const WorkloadSettings & old_settings,
        const WorkloadSettings & new_settings,
        SharingMode sharing_mode)
    {
        switch (sharing_mode)
        {
            case SharingMode::TimeShared: return old_parent != new_parent || old_settings.priority != new_settings.priority;
            case SharingMode::SpaceShared: return old_parent != new_parent || old_settings.precedence != new_settings.precedence;
        }
    }

    /// Returns the cost units of the resource associated with this node
    virtual CostUnit getCostUnit() const = 0;

    /// Returns the name of the workload associated with this node
    virtual const String & getWorkload() const = 0;

    /// Returns the name of the resource associated with this node
    virtual const String & getResource() const = 0;

    /// Returns current settings of this workload node
    virtual const WorkloadSettings & getSettings() const = 0;

    /// Returns the link to be used to access the queue or `nullptr` if workload is not a leaf workload
    virtual ResourceLink getLink() const = 0;

    /// Collects nodes that could be accessed with raw pointers by resource requests (queue and constraints)
    /// NOTE: This is a building block for classifier. Note that due to possible movement of a queue, set of constraints
    /// for that queue might change in future, and `request->constraints` might reference nodes not in
    /// the initial set of nodes returned by `addRawPointerNodes()`. To avoid destruction of such additional nodes
    /// classifier must (indirectly) hold nodes return by `addRawPointerNodes()` for all future versions of
    /// all workload nodes. Such a version control is done by `WorkloadResourceManager`.
    virtual void addRawPointerNodes(std::vector<SchedulerNodePtr> & nodes) = 0;

    /// Returns true if this workload node has workload children
    virtual bool hasWorkloadChildren() const = 0;

    /// Introspection. Calls a visitor for self and every internal node. Do not recurse into workload children.
    virtual void forEachSchedulerNode(std::function<void(ISchedulerNode *)> visitor) = 0;
};


}
