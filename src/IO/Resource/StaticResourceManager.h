#pragma once

#include <IO/IResourceManager.h>
#include <IO/SchedulerRoot.h>
#include <IO/Resource/ClassifiersConfig.h>

#include <mutex>

namespace DB
{

/*
 * Reads `<resources>` from config at startup and registers them in single `SchedulerRoot`.
 * Do not support configuration updates, server restart is required.
 */
class StaticResourceManager : public IResourceManager
{
public:
    // Just initialization, any further updates are ignored for the sake of simplicity
    // NOTE: manager must be initialized before any acquire() calls to avoid races
    void updateConfiguration(const Poco::Util::AbstractConfiguration & config) override;

    ClassifierPtr acquire(const String & classifier_name) override;

private:
    struct Resource
    {
        std::unordered_map<String, SchedulerNodePtr> nodes; // by paths

        Resource(
            const String & name,
            EventQueue * event_queue,
            const Poco::Util::AbstractConfiguration & config,
            const std::string & config_prefix);
    };

    struct Classifier : public IClassifier
    {
        Classifier(const StaticResourceManager & manager, const ClassifierDescription & cfg);
        ResourceLink get(const String & resource_name) override;
        std::unordered_map<String, ResourceLink> resources; // accessible resources by names
    };

    SchedulerRoot scheduler;
    std::unordered_map<String, Resource> resources; // by name
    std::unique_ptr<ClassifiersConfig> classifiers;
};

}
