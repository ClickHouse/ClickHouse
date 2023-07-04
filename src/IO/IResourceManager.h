#pragma once

#include <IO/ResourceLink.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <unordered_map>

namespace DB
{

/*
 * Instance of derived class holds everything required for resource consumption,
 * including resources currently registered at `SchedulerRoot`. This is required to avoid
 * problems during configuration update. Do not hold instances longer than required.
 * Should be created on query start and destructed when query is done.
 */
class IClassifier : private boost::noncopyable
{
public:
    virtual ~IClassifier() {}

    /// Returns ResouceLink that should be used to access resource.
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
    virtual ~IResourceManager() {}

    /// Initialize or reconfigure manager.
    virtual void updateConfiguration(const Poco::Util::AbstractConfiguration & config) = 0;

    /// Obtain a classifier instance required to get access to resources.
    /// Note that it holds resource configuration, so should be destructed when query is done.
    virtual ClassifierPtr acquire(const String & classifier_name) = 0;
};

using ResourceManagerPtr = std::shared_ptr<IResourceManager>;

}
