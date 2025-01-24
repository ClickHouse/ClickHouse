#pragma once

#include <Common/Scheduler/ResourceLink.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <functional>

namespace DB
{

class ISchedulerNode;
using SchedulerNodePtr = std::shared_ptr<ISchedulerNode>;

/*
 * Instance of derived class holds everything required for resource consumption,
 * including resources currently registered at `SchedulerRoot`. This is required to avoid
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
};

using ClassifierPtr = std::shared_ptr<IClassifier>;

/*
 * Represents control plane of resource scheduling. Derived class is responsible for reading
 * configuration, creating all required `ISchedulerNode` objects and
 * managing their lifespan.
 */
class IResourceManager : private boost::noncopyable
{
public:
    virtual ~IResourceManager() = default;

    /// Initialize or reconfigure manager.
    virtual void updateConfiguration(const Poco::Util::AbstractConfiguration & config) = 0;

    /// Returns true iff given resource is controlled through this manager.
    virtual bool hasResource(const String & resource_name) const = 0;

    /// Obtain a classifier instance required to get access to resources.
    /// Note that it holds resource configuration, so should be destructed when query is done.
    virtual ClassifierPtr acquire(const String & classifier_name) = 0;

    /// For introspection, see `system.scheduler` table
    using VisitorFunc = std::function<void(const String & resource, const String & path, ISchedulerNode * node)>;
    virtual void forEachNode(VisitorFunc visitor) = 0;
};

using ResourceManagerPtr = std::shared_ptr<IResourceManager>;

}
