#pragma once

#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/WorkloadSettings.h>

#include <Common/Priority.h>

#include <base/defines.h>
#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <boost/noncopyable.hpp>
#include <boost/intrusive/list.hpp>

#include <memory>


namespace DB
{

class ISchedulerNode;
class IWorkloadNode;
class EventQueue;
using EventId = UInt64;

inline const Poco::Util::AbstractConfiguration & emptyConfig()
{
    static Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    return *config;
}

/// Common configuration info for scheduler nodes
struct SchedulerNodeInfo
{
    double weight = 1.0; /// Weight of this node among its siblings
    Priority priority; /// Priority of this node among its siblings (lower value means higher priority) for time-shared resources
    Priority precedence; /// Precedence of this node among its siblings (lower value means higher precedence) for space-shared resources

    SchedulerNodeInfo() = default;

    explicit SchedulerNodeInfo(double weight_, Priority priority_ = {}, Priority precedence_ = {});
    explicit SchedulerNodeInfo(const WorkloadSettings & settings);
    explicit SchedulerNodeInfo(const Poco::Util::AbstractConfiguration & config, const String & config_prefix = {});

    SchedulerNodeInfo & setWeight(double value);
    SchedulerNodeInfo & setPriority(Int64 value);
    SchedulerNodeInfo & setPriority(Priority value);
    SchedulerNodeInfo & setPrecedence(Int64 value);
    SchedulerNodeInfo & setPrecedence(Priority value);

    void update(const WorkloadSettings & new_settings);

    // To check if configuration update required
    bool equals(const SchedulerNodeInfo & o) const;
};

/// Node of hierarchy for scheduling requests for resource. Base class for all
/// kinds of scheduling elements (queues, policies, constraints and schedulers).
///
/// Root node is a scheduler, which has it's thread to process requests and events.
/// (see ResourceRequest, IncreaseRequest, DecreaseRequest and EventQueue for details).
/// Immediate children of the root represent independent resources (usually, only one).
/// Each resource has it's own hierarchy to achieve required scheduling policies.
/// Non-leaf nodes keep scheduling state (usage history, amount of in-flight requests,
/// allocation sizes, etc). Leafs of hierarchy are queues capable of holding
/// pending resource requests.
///
/// Resource can be either time-shared (e.g. CPU) or space-shared (e.g. Memory).
/// ITimeSharedNode and ISpaceSharedNode are used respectively.
/// The scheduling hierarchy cannot mix nodes of both kinds.
///
/// This base class provides common functionality for all nodes:
/// - hierarchy construction: attaching/detaching children
/// - working with EventQueue: schedule/cancel/process activations
///
/// All methods must be called only from scheduler thread for thread-safety.
/// ___________________________________________________________________________
///                         |
///   SCHEDULING HIERARCHY  |         ISchedulerNode DERIVED CLASS
/// ________________________|__________________________________________________
///                         |                       |
///        scheduler        | TimeSharedScheduler   | SpaceSharedScheduler
///            |            |_______________________|__________________________
///            |            |
///           all           | WorkloadNode<BaseNode> (workload root)
///            |            |__________________________________________________
///            |            |                       |
///        constraint       | SemaphoreConstraint,  | AllocationLimit
///            |            | ThrottlerConstraint   |
///            |            |                       |
///          policy         | PriorityPolicy,       | AllocationPriorityPolicy
///          /   \          | FairPolicy            | AllocationFairPolicy
///         /     \         |_______________________|__________________________
///        /       \        |
///      dev       prod     | WorkloadNode<BaseNode> (workload root)
///       |         |       |__________________________________________________
///       |         |       |                       |
///     fifo       fifo     | FifoQueue             | AllocationQueue
/// ________________________|_______________________|__________________________
///                         |                       |
///             RESOURCE:   | CPU, IO, QuerySlot    | MemoryReservation
/// ________________________|_______________________|__________________________
class ISchedulerNode : private boost::noncopyable
{
public:
    explicit ISchedulerNode(
        EventQueue & event_queue_,
        const Poco::Util::AbstractConfiguration & config = emptyConfig(),
        const String & config_prefix = {});

    ISchedulerNode(
        EventQueue & event_queue_,
        const SchedulerNodeInfo & info_);

    virtual ~ISchedulerNode();

    virtual const String & getTypeName() const = 0;

    /// Checks if two nodes configuration is equal
    virtual bool equals(ISchedulerNode * other);

    /// Attach new child
    virtual void attachChild(const std::shared_ptr<ISchedulerNode> & child) = 0;

    /// Detach child
    /// NOTE: child might be destroyed if the only reference was stored in parent
    virtual void removeChild(ISchedulerNode * child) = 0;

    /// Get attached child by name (for tests only)
    virtual ISchedulerNode * getChild(const String & child_name) = 0;

    /// Returns full path string using names of every parent
    String getPath() const;

    /// Attach to a parent (used by attachChild)
    void setParentNode(ISchedulerNode * parent_);

    /// Return workload name this node belongs to (if any)
    const String & getWorkloadName() const;

    /// Return resource name this node belongs to (if any)
    const String & getResourceName() const;

    /// Return readable string for a cost value
    String formatReadableCost(ResourceCost cost) const;

    EventQueue & event_queue;
    String basename;
    SchedulerNodeInfo info;
    ISchedulerNode * parent = nullptr;
    IWorkloadNode * workload = nullptr; /// Workload node this scheduler node belongs to (if any)

protected:
    /// Notify parents about the first pending request or constraint becoming satisfied.
    /// Postponed to be handled in scheduler thread, so it is intended to be called from outside.
    void scheduleActivation();

    /// Cancel previously scheduled activation.
    void cancelActivation();

    /// Do activation of this node.
    /// This is the way to introduce updates into the hierarchy from outside.
    /// It is called from scheduler thread's in response to scheduleActivation() by the EventQueue.
    virtual void processActivation() {}

private:
    friend class EventQueue;
    EventId activation_event_id = 0; // Non-zero for `ISchedulerNode` placed in EventQueue::activations
    boost::intrusive::list_member_hook<> activation_hook;
    using ActivationHook = boost::intrusive::member_hook<ISchedulerNode, boost::intrusive::list_member_hook<>, &ISchedulerNode::activation_hook>;
    using ActivationList = boost::intrusive::list<ISchedulerNode, ActivationHook>;
};

using SchedulerNodePtr = std::shared_ptr<ISchedulerNode>;

}
