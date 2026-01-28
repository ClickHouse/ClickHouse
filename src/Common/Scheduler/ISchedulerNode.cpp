#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/IWorkloadNode.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/CostUnit.h>
#include <Common/EventRateMeter.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

SchedulerNodeInfo::SchedulerNodeInfo(double weight_, Priority priority_, Priority precedence_)
{
    setWeight(weight_);
    setPriority(priority_);
    setPrecedence(precedence_);
}

SchedulerNodeInfo::SchedulerNodeInfo(const WorkloadSettings & settings)
    : SchedulerNodeInfo(settings.weight, settings.priority, settings.precedence)
{}

// TODO(serxa): this is legacy, get rid of it together with CustomResourceManager
SchedulerNodeInfo::SchedulerNodeInfo(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    setWeight(config.getDouble(config_prefix + ".weight", weight));
    setPriority(config.getInt64(config_prefix + ".priority", priority));
}

SchedulerNodeInfo & SchedulerNodeInfo::setWeight(double value)
{
    if (value <= 0 || !isfinite(value))
        throw Exception(
            ErrorCodes::INVALID_SCHEDULER_NODE,
            "Zero, negative and non-finite node weights are not allowed: {}",
            value);
    weight = value;
    return *this;
}

SchedulerNodeInfo & SchedulerNodeInfo::setPriority(Int64 value)
{
    priority.value = value;
    return *this;
}

SchedulerNodeInfo & SchedulerNodeInfo::setPriority(Priority value)
{
    priority = value;
    return *this;
}

SchedulerNodeInfo & SchedulerNodeInfo::setPrecedence(Int64 value)
{
    precedence.value = value;
    return *this;
}

SchedulerNodeInfo & SchedulerNodeInfo::setPrecedence(Priority value)
{
    precedence = value;
    return *this;
}

void SchedulerNodeInfo::update(const WorkloadSettings & new_settings)
{
    setWeight(new_settings.weight);
    setPriority(new_settings.priority);
    setPrecedence(new_settings.precedence);
}

bool SchedulerNodeInfo::equals(const SchedulerNodeInfo & o) const
{
    // `parent` data is not compared intentionally (it is not part of configuration settings)
    return weight == o.weight && priority == o.priority && precedence == o.precedence;
}

ISchedulerNode::ISchedulerNode(EventQueue & event_queue_, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    : event_queue(event_queue_)
    , info(config, config_prefix)
{}

ISchedulerNode::ISchedulerNode(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
    : event_queue(event_queue_)
    , info(info_)
{}

ISchedulerNode::~ISchedulerNode()
{
    // Make sure there is no dangling reference in activations queue
    cancelActivation();
}

bool ISchedulerNode::equals(ISchedulerNode * other)
{
    return info.equals(other->info);
}

String ISchedulerNode::getPath() const
{
    String result;
    const ISchedulerNode * ptr = this;
    while (ptr->parent)
    {
        result = "/" + ptr->basename + result;
        ptr = ptr->parent;
    }
    return result.empty() ? "/" : result;
}

void ISchedulerNode::setParentNode(ISchedulerNode * parent_)
{
    parent = parent_;
    // Avoid activation of a detached node
    if (parent == nullptr)
        cancelActivation();
}

const String & ISchedulerNode::getWorkloadName() const
{
    if (workload)
        return workload->getWorkload();
    static const String empty;
    return empty;
}

const String & ISchedulerNode::getResourceName() const
{
    if (workload)
        return workload->getResource();
    static const String empty;
    return empty;
}

String ISchedulerNode::formatReadableCost(ResourceCost cost) const
{
    return DB::formatReadableCost(cost, workload ? workload->getCostUnit() : CostUnit::IOByte);
}

void ISchedulerNode::scheduleActivation()
{
    if (likely(parent))
    {
        // The same as `enqueue([this] { parent->activateChild(*this); });` but faster
        event_queue.enqueueActivation(*this);
    }
}

void ISchedulerNode::cancelActivation()
{
    event_queue.cancelActivation(*this);
}

}
