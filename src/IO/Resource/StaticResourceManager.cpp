#include <IO/Resource/StaticResourceManager.h>

#include <IO/SchedulerNodeFactory.h>
#include <IO/ResourceManagerFactory.h>
#include <IO/ISchedulerQueue.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>

#include <map>
#include <tuple>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
    extern const int RESOURCE_NOT_FOUND;
    extern const int INVALID_SCHEDULER_NODE;
}

StaticResourceManager::Resource::Resource(
    const String & name,
    EventQueue * event_queue,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix)
{
    // Initialize scheduler nodes
    Poco::Util::AbstractConfiguration::Keys keys;
    std::sort(keys.begin(), keys.end()); // for parents to appear before children
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        if (!startsWith(key, "node"))
            continue;

        // Validate path
        String path = config.getString(config_prefix + "." + key + "[@path]", "");
        if (path.empty())
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Attribute 'path' must be specified in all nodes for resource '{}'", name);
        if (path[0] != '/')
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "path must start with '/' for resource '{}'", name);

        // Create node
        String type = config.getString(config_prefix + "." + key + ".type", "fifo");
        SchedulerNodePtr node = SchedulerNodeFactory::instance().get(type, event_queue, config, config_prefix + "." + key);
        node->basename = path.substr(1);

        // Take ownership
        if (auto [_, inserted] = nodes.emplace(path, node); !inserted)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Duplicate path '{}' for resource '{}'", path, name);

        // Attach created node to parent (if not root)
        if (path != "/")
        {
            String parent_path = path.substr(0, path.rfind('/'));
            if (parent_path.empty())
                parent_path = "/";
            if (auto parent = nodes.find(parent_path); parent != nodes.end())
                parent->second->attachChild(node);
            else
                throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Parent doesn't exist for path '{}' for resource '{}'", path, name);
        }
    }

    if (nodes.find("/") == nodes.end())
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "undefined root node path '/' for resource '{}'", name);
}

StaticResourceManager::Classifier::Classifier(const StaticResourceManager & manager, const ClassifierDescription & cfg)
{
    for (auto [resource_name, path] : cfg)
    {
        if (auto resource_iter = manager.resources.find(resource_name); resource_iter != manager.resources.end())
        {
            const Resource & resource = resource_iter->second;
            if (auto node_iter = resource.nodes.find(path); node_iter != resource.nodes.end())
            {
                if (auto * queue = dynamic_cast<ISchedulerQueue *>(node_iter->second.get()))
                    resources.emplace(resource_name, ResourceLink{.queue = queue});
                else
                    throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Unable to access non-queue node at path '{}' for resource '{}'", path, resource_name);
            }
            else
                throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Path '{}' for resource '{}' does not exist", path, resource_name);
        }
        else
            resources.emplace(resource_name, ResourceLink{}); // resource not configured - unlimited
    }
}

ResourceLink StaticResourceManager::Classifier::get(const String & resource_name)
{
    if (auto iter = resources.find(resource_name); iter != resources.end())
        return iter->second;
    else
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Access denied to resource '{}'", resource_name);
}

void StaticResourceManager::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    if (!resources.empty())
        return; // already initialized, configuration update is not supported

    Poco::Util::AbstractConfiguration::Keys keys;
    const String config_prefix = "resources";
    config.keys(config_prefix, keys);

    // Create resource for every element under <resources> tag
    for (const auto & key : keys)
    {
        auto [iter, _] = resources.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple(key, scheduler.event_queue, config, config_prefix + "." + key));
        // Attach root of resource to scheduler
        scheduler.attachChild(iter->second.nodes.find("/")->second);
    }

    // Initialize classifiers
    classifiers = std::make_unique<ClassifiersConfig>(config);

    // Run scheduler thread
    scheduler.start();
}

ClassifierPtr StaticResourceManager::acquire(const String & classifier_name)
{
    return std::make_shared<Classifier>(*this, classifiers->get(classifier_name));
}

void registerStaticResourceManager(ResourceManagerFactory & factory)
{
    factory.registerMethod<StaticResourceManager>("static");
}

}
