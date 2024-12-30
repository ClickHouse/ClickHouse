#include <Common/Scheduler/createResourceManager.h>
#include <Common/Scheduler/Nodes/CustomResourceManager.h>
#include <Common/Scheduler/Nodes/IOResourceManager.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <memory>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

class ResourceManagerDispatcher : public IResourceManager
{
private:
    class Classifier : public IClassifier
    {
    public:
        void addClassifier(const ClassifierPtr & classifier)
        {
            classifiers.push_back(classifier);
        }

        bool has(const String & resource_name) override
        {
            for (const auto & classifier : classifiers)
            {
                if (classifier->has(resource_name))
                    return true;
            }
            return false;
        }

        ResourceLink get(const String & resource_name) override
        {
            for (auto & classifier : classifiers)
            {
                if (classifier->has(resource_name))
                    return classifier->get(resource_name);
            }
            throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Access denied to resource '{}'", resource_name);
        }
    private:
        std::vector<ClassifierPtr> classifiers; // should be constant after initialization to avoid races
    };

public:
    void addManager(const ResourceManagerPtr & manager)
    {
        managers.push_back(manager);
    }

    void updateConfiguration(const Poco::Util::AbstractConfiguration & config) override
    {
        for (auto & manager : managers)
            manager->updateConfiguration(config);
    }

    bool hasResource(const String & resource_name) const override
    {
        for (const auto & manager : managers)
        {
            if (manager->hasResource(resource_name))
                return true;
        }
        return false;
    }

    ClassifierPtr acquire(const String & workload_name) override
    {
        auto classifier = std::make_shared<Classifier>();
        for (const auto & manager : managers)
            classifier->addClassifier(manager->acquire(workload_name));
        return classifier;
    }

    void forEachNode(VisitorFunc visitor) override
    {
        for (const auto & manager : managers)
            manager->forEachNode(visitor);
    }

private:
    std::vector<ResourceManagerPtr> managers; // Should be constant after initialization to avoid races
};

ResourceManagerPtr createResourceManager(const ContextMutablePtr & global_context)
{
    auto dispatcher = std::make_shared<ResourceManagerDispatcher>();

    // NOTE: if the same resource is described by both managers, then manager added earlier will be used.
    dispatcher->addManager(std::make_shared<CustomResourceManager>());
    dispatcher->addManager(std::make_shared<IOResourceManager>(global_context->getWorkloadEntityStorage()));

    return dispatcher;
}

}
