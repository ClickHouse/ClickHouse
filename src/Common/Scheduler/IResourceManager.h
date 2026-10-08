#pragma once

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/Priority.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <functional>

namespace DB
{

class ISchedulerNode;
using SchedulerNodePtr = std::shared_ptr<ISchedulerNode>;

struct ClassifierSettings
{
    bool throw_on_unknown_workload = false;

    /// Per-query scheduling parameters the classifier builds its `ResourceSchedulingContext` from
    /// (defaults describe a query that set nothing).
    Float64 weight = 1.0;
    Float64 weight_lowering_factor = 1.0;
    Float64 weight_lowering_age_seconds = 0.0;
    Float64 weight_lowering_cpu_seconds = 0.0;
    Float64 weight_lowering_io_bytes = 0.0;
    Priority priority;
};

/*
 * Instance of derived class holds everything required for resource consumption,
 * including resources currently registered at the scheduler. This is required to avoid
 * problems during configuration update. Do not hold instances longer than required.
 * Should be created on query start and destructed when query is done.
 */
class IClassifier : private boost::noncopyable
{
public:
    virtual ~IClassifier() = default;

    /// Returns true iff resource access is allowed by this classifier
    virtual bool has(const String & resource_name) = 0;

    /// Returns ResourceLink that should be used to access resource.
    /// Returned link is valid until classifier destruction.
    virtual ResourceLink get(const String & resource_name) = 0;
    /// Returns settings that should be used to limit workload on given resource.
    virtual WorkloadSettings getWorkloadSettings(const String & resource_name) const = 0;
};

using ClassifierPtr = std::shared_ptr<IClassifier>;

/// Resolved server-limit configuration pushed into the resource manager: the server-wide CPU/memory
/// limits plus the flags that enable mirroring them and synthesizing the `default` workload. Limit
/// values are already reduced from the raw server settings (`*_num`/`*_ratio_to_cores`/`*_to_ram_ratio`)
/// to a single effective number; a limit that is off or unbounded is `WorkloadSettings::unlimited`.
struct ResourceManagerConfiguration
{
    /// When true, the server CPU-concurrency limit is enforced through the workload scheduler: a
    /// combined `MASTER THREAD, WORKER THREAD` resource is created if the operator declared none,
    /// and the implicit root's `max_concurrent_threads` is set to `cpu_slots`.
    bool respect_cpu_limit = false;

    /// When true, the server memory limit is enforced as a reservation-admission budget through the
    /// workload scheduler: a `MEMORY RESERVATION` resource is created if the operator declared none,
    /// and the implicit root's `max_memory` is set to `memory_bytes`.
    bool respect_memory_limit = false;

    /// When true, a `default` workload is synthesized if none is defined explicitly.
    bool implicit_default_workload = false;

    /// Effective CPU-slot budget (number of concurrent query threads).
    Int64 cpu_slots = WorkloadSettings::unlimited;

    /// Effective memory-reservation budget in bytes.
    Int64 memory_bytes = WorkloadSettings::unlimited;
};

/*
 * Represents control plane of resource scheduling. Derived class is responsible for reading
 * configuration, creating all required `ISchedulerNode` objects and
 * managing their lifespan.
 */
class IResourceManager : private boost::noncopyable
{
public:
    virtual ~IResourceManager() = default;

    /// Returns true iff given resource is controlled through this manager.
    virtual bool hasResource(const String & resource_name) const = 0;

    /// Obtain a classifier instance required to get access to resources.
    /// Note that it holds resource configuration, so should be destructed when query is done.
    virtual ClassifierPtr acquire(const String & classifier_name, const ClassifierSettings & settings) = 0;

    ClassifierPtr acquire(const String & classifier_name)
    {
        return acquire(classifier_name, {});
    }

    /// For introspection, see `system.scheduler` table
    using VisitorFunc = std::function<void(const String & resource, const String & path, ISchedulerNode * node)>;
    virtual void forEachNode(VisitorFunc visitor) = 0;

    /// Applies the server-limit configuration to the per-resource implicit root workloads (and
    /// synthesizes the `default` workload when requested). Called on startup and on every
    /// server-settings reload. The default implementation does nothing for managers that do not
    /// support server-limit mirroring.
    virtual void updateConfiguration(const ResourceManagerConfiguration &) {}
};

using ResourceManagerPtr = std::shared_ptr<IResourceManager>;

}
