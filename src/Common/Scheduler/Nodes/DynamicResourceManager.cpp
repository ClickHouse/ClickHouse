#include <Common/Scheduler/Nodes/DynamicResourceManager.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/ResourceManagerFactory.h>
#include <Common/Scheduler/ISchedulerQueue.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <map>
#include <tuple>
#include <future>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
    extern const int RESOURCE_NOT_FOUND;
    extern const int INVALID_SCHEDULER_NODE;
}

DynamicResourceManager::State::State(EventQueue * event_queue, const Poco::Util::AbstractConfiguration & config)
    : classifiers(config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    const String config_prefix = "resources";
    config.keys(config_prefix, keys);

    // Create resource for every element under <resources> tag
    for (const auto & key : keys)
    {
        resources.emplace(key, std::make_shared<Resource>(key, event_queue, config, config_prefix + "." + key));
    }
}

DynamicResourceManager::State::Resource::Resource(
    const String & name,
    EventQueue * event_queue,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    // Sort nodes by path to create parents before children
    std::map<String, String> path2key;
    for (const auto & key : keys)
    {
        if (!startsWith(key, "node"))
            continue;
        String path = config.getString(config_prefix + "." + key + "[@path]", "");
        if (path.empty())
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Attribute 'path' must be specified in all nodes for resource '{}'", name);
        if (path[0] != '/')
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Path must start with '/' for resource '{}'", name);
        if (auto [_, inserted] = path2key.emplace(path, key); !inserted)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Duplicate path '{}' for resource '{}'", path, name);
    }

    // Create nodes
    bool has_root = false;
    for (const auto & [path, key] : path2key)
    {
        // Validate path
        size_t slash = path.rfind('/');
        if (slash == String::npos)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Invalid scheduler node path '{}' for resource '{}'", path, name);

        // Create node
        String basename = path.substr(slash + 1); // root name is empty string
        auto [iter, _] = nodes.emplace(path, Node(basename, event_queue, config, config_prefix + "." + key));
        if (path == "/")
        {
            has_root = true;
            continue;
        }

        // Attach created node to parent (if not root)
        // NOTE: resource root is attached to the scheduler using event queue for thread-safety
        String parent_path = path.substr(0, slash);
        if (parent_path.empty())
            parent_path = "/";
        if (auto parent = nodes.find(parent_path); parent != nodes.end())
            parent->second.ptr->attachChild(iter->second.ptr);
        else
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Parent node doesn't exist for path '{}' for resource '{}'", path, name);
    }

    if (!has_root)
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "undefined root node path '/' for resource '{}'", name);
}

DynamicResourceManager::State::Resource::~Resource()
{
    // NOTE: we should rely on `attached_to` and cannot use `parent`,
    // NOTE: because `parent` can be `nullptr` in case attachment is still in event queue
    if (attached_to != nullptr)
    {
        ISchedulerNode * root = nodes.find("/")->second.ptr.get();
        attached_to->event_queue->enqueue([my_scheduler = attached_to, root]
        {
            my_scheduler->removeChild(root);
        });
    }
}

DynamicResourceManager::State::Node::Node(const String & name, EventQueue * event_queue, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    : type(config.getString(config_prefix + ".type", "fifo"))
    , ptr(SchedulerNodeFactory::instance().get(type, event_queue, config, config_prefix))
{
    ptr->basename = name;
}

bool DynamicResourceManager::State::Resource::equals(const DynamicResourceManager::State::Resource & o) const
{
    if (nodes.size() != o.nodes.size())
        return false;

    for (const auto & [path, o_node] : o.nodes)
    {
        auto iter = nodes.find(path);
        if (iter == nodes.end())
            return false;
        if (!iter->second.equals(o_node))
            return false;
    }

    return true;
}

bool DynamicResourceManager::State::Node::equals(const DynamicResourceManager::State::Node & o) const
{
    if (type != o.type)
        return false;
    return ptr->equals(o.ptr.get());
}

DynamicResourceManager::Classifier::Classifier(const DynamicResourceManager::StatePtr & state_, const String & classifier_name)
    : state(state_)
{
    // State is immutable, but nodes are mutable and thread-safe
    // So it's safe to obtain node pointers w/o lock
    for (auto [resource_name, path] : state->classifiers.get(classifier_name))
    {
        if (auto resource_iter = state->resources.find(resource_name); resource_iter != state->resources.end())
        {
            const auto & resource = resource_iter->second;
            if (auto node_iter = resource->nodes.find(path); node_iter != resource->nodes.end())
            {
                if (auto * queue = dynamic_cast<ISchedulerQueue *>(node_iter->second.ptr.get()))
                    resources.emplace(resource_name, ResourceLink{.queue = queue});
                else
                    throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Unable to access non-queue node at path '{}' for resource '{}'", path, resource_name);
            }
            else
                throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Path '{}' for resource '{}' does not exist", path, resource_name);
        }
        else
            resources.emplace(resource_name, ResourceLink{}); // resource not configured yet - use unlimited resource
    }
}

ResourceLink DynamicResourceManager::Classifier::get(const String & resource_name)
{
    if (auto iter = resources.find(resource_name); iter != resources.end())
        return iter->second;
    throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Access denied to resource '{}'", resource_name);
}

DynamicResourceManager::DynamicResourceManager()
    : state(new State())
{
    scheduler.start();
}

void DynamicResourceManager::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    StatePtr new_state = std::make_shared<State>(scheduler.event_queue, config);

    std::lock_guard lock{mutex};

    // Resource update leads to loss of runtime data of nodes and may lead to temporary violation of constraints (e.g. limits)
    // Try to minimise this by reusing "equal" resources (initialized with the same configuration).
    std::vector<State::ResourcePtr> resources_to_attach;
    for (auto & [name, new_resource] : new_state->resources)
    {
        if (auto iter = state->resources.find(name); iter != state->resources.end()) // Resource update
        {
            State::ResourcePtr old_resource = iter->second;
            if (old_resource->equals(*new_resource))
            {
                new_resource = old_resource; // Rewrite with older version to avoid loss of runtime data
                continue;
            }
        }
        // It is new or updated resource
        resources_to_attach.emplace_back(new_resource);
    }

    // Commit new state
    // NOTE: dtor will detach from scheduler old resources that are not in use currently
    state = new_state;

    // Attach new and updated resources to the scheduler
    for (auto & resource : resources_to_attach)
    {
        const SchedulerNodePtr & root = resource->nodes.find("/")->second.ptr;
        resource->attached_to = &scheduler;
        scheduler.event_queue->enqueue([this, root]
        {
            scheduler.attachChild(root);
        });
    }

    // NOTE: after mutex unlock `state` became available for Classifier(s) and must be immutable
}

ClassifierPtr DynamicResourceManager::acquire(const String & classifier_name)
{
    // Acquire a reference to the current state
    StatePtr state_ref;
    {
        std::lock_guard lock{mutex};
        state_ref = state;
    }

    return std::make_shared<Classifier>(state_ref, classifier_name);
}

void DynamicResourceManager::forEachNode(IResourceManager::VisitorFunc visitor)
{
    // Acquire a reference to the current state
    StatePtr state_ref;
    {
        std::lock_guard lock{mutex};
        state_ref = state;
    }

    std::promise<void> promise;
    auto future = promise.get_future();
    scheduler.event_queue->enqueue([state_ref, visitor, &promise]
    {
        for (auto & [name, resource] : state_ref->resources)
            for (auto & [path, node] : resource->nodes)
                visitor(name, path, node.type, node.ptr);
        promise.set_value();
    });

    // Block until execution is done in the scheduler thread
    future.get();
}

void registerDynamicResourceManager(ResourceManagerFactory & factory)
{
    factory.registerMethod<DynamicResourceManager>("dynamic");
}

}
