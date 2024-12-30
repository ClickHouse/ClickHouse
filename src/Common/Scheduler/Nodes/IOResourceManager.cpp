#include <Common/Scheduler/Nodes/IOResourceManager.h>

#include <Common/Scheduler/Nodes/FifoQueue.h>
#include <Common/Scheduler/Nodes/FairPolicy.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/Priority.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>

#include <memory>
#include <mutex>
#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int LOGICAL_ERROR;
}

namespace
{
    String getEntityName(const ASTPtr & ast)
    {
        if (auto * create = typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
            return create->getWorkloadName();
        if (auto * create = typeid_cast<ASTCreateResourceQuery *>(ast.get()))
            return create->getResourceName();
        return "unknown-workload-entity";
    }
}

IOResourceManager::NodeInfo::NodeInfo(const ASTPtr & ast, const String & resource_name)
{
    auto * create = assert_cast<ASTCreateWorkloadQuery *>(ast.get());
    name = create->getWorkloadName();
    parent = create->getWorkloadParent();
    settings.updateFromChanges(create->changes, resource_name);
}

IOResourceManager::Resource::Resource(const ASTPtr & resource_entity_)
    : resource_entity(resource_entity_)
    , resource_name(getEntityName(resource_entity))
{
    scheduler.start();
}

IOResourceManager::Resource::~Resource()
{
    scheduler.stop();
}

void IOResourceManager::Resource::createNode(const NodeInfo & info)
{
    if (info.name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Workload must have a name in resource '{}'",
            resource_name);

    if (info.name == info.parent)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Self-referencing workload '{}' is not allowed in resource '{}'",
            info.name, resource_name);

    if (node_for_workload.contains(info.name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node for creating workload '{}' already exist in resource '{}'",
            info.name, resource_name);

    if (!info.parent.empty() && !node_for_workload.contains(info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parent node '{}' for creating workload '{}' does not exist in resource '{}'",
            info.parent, info.name, resource_name);

    if (info.parent.empty() && root_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The second root workload '{}' is not allowed (current root '{}') in resource '{}'",
            info.name, root_node->basename, resource_name);

    executeInSchedulerThread([&, this]
    {
        auto node = std::make_shared<UnifiedSchedulerNode>(scheduler.event_queue, info.settings);
        node->basename = info.name;
        if (!info.parent.empty())
            node_for_workload[info.parent]->attachUnifiedChild(node);
        else
        {
            root_node = node;
            scheduler.attachChild(root_node);
        }
        node_for_workload[info.name] = node;

        updateCurrentVersion();
    });
}

void IOResourceManager::Resource::deleteNode(const NodeInfo & info)
{
    if (!node_for_workload.contains(info.name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node for removing workload '{}' does not exist in resource '{}'",
            info.name, resource_name);

    if (!info.parent.empty() && !node_for_workload.contains(info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parent node '{}' for removing workload '{}' does not exist in resource '{}'",
            info.parent, info.name, resource_name);

    auto node = node_for_workload[info.name];

    if (node->hasUnifiedChildren())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Removing workload '{}' with children in resource '{}'",
        info.name, resource_name);

    executeInSchedulerThread([&]
    {
        if (!info.parent.empty())
            node_for_workload[info.parent]->detachUnifiedChild(node);
        else
        {
            chassert(node == root_node);
            scheduler.removeChild(root_node.get());
            root_node.reset();
        }

        node_for_workload.erase(info.name);

        updateCurrentVersion();
    });
}

void IOResourceManager::Resource::updateNode(const NodeInfo & old_info, const NodeInfo & new_info)
{
    if (old_info.name != new_info.name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Updating a name of workload '{}' to '{}' is not allowed in resource '{}'",
            old_info.name, new_info.name, resource_name);

    if (old_info.parent != new_info.parent && (old_info.parent.empty() || new_info.parent.empty()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Workload '{}' invalid update of parent from '{}' to '{}' in resource '{}'",
            old_info.name, old_info.parent, new_info.parent, resource_name);

    if (!node_for_workload.contains(old_info.name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node for updating workload '{}' does not exist in resource '{}'",
            old_info.name, resource_name);

    if (!old_info.parent.empty() && !node_for_workload.contains(old_info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Old parent node '{}' for updating workload '{}' does not exist in resource '{}'",
            old_info.parent, old_info.name, resource_name);

    if (!new_info.parent.empty() && !node_for_workload.contains(new_info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "New parent node '{}' for updating workload '{}' does not exist in resource '{}'",
            new_info.parent, new_info.name, resource_name);

    executeInSchedulerThread([&, this]
    {
        auto node = node_for_workload[old_info.name];
        bool detached = false;
        if (UnifiedSchedulerNode::updateRequiresDetach(old_info.parent, new_info.parent, old_info.settings, new_info.settings))
        {
            if (!old_info.parent.empty())
                node_for_workload[old_info.parent]->detachUnifiedChild(node);
            detached = true;
        }

        node->updateSchedulingSettings(new_info.settings);

        if (detached)
        {
            if (!new_info.parent.empty())
                node_for_workload[new_info.parent]->attachUnifiedChild(node);
        }
        updateCurrentVersion();
    });
}

void IOResourceManager::Resource::updateCurrentVersion()
{
    auto previous_version = current_version;

    // Create a full list of constraints and queues in the current hierarchy
    current_version = std::make_shared<Version>();
    if (root_node)
        root_node->addRawPointerNodes(current_version->nodes);

    // See details in version control section of description in IOResourceManager.h
    if (previous_version)
    {
        previous_version->newer_version = current_version;
        previous_version.reset(); // Destroys previous version nodes if there are no classifiers referencing it
    }
}

IOResourceManager::Workload::Workload(IOResourceManager * resource_manager_, const ASTPtr & workload_entity_)
    : resource_manager(resource_manager_)
    , workload_entity(workload_entity_)
{
    try
    {
        for (auto & [resource_name, resource] : resource_manager->resources)
            resource->createNode(NodeInfo(workload_entity, resource_name));
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error in IOResourceManager: {}",
            getCurrentExceptionMessage(/* with_stacktrace = */ true));
    }
}

IOResourceManager::Workload::~Workload()
{
    try
    {
        for (auto & [resource_name, resource] : resource_manager->resources)
            resource->deleteNode(NodeInfo(workload_entity, resource_name));
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error in IOResourceManager: {}",
            getCurrentExceptionMessage(/* with_stacktrace = */ true));
    }
}

void IOResourceManager::Workload::updateWorkload(const ASTPtr & new_entity)
{
    try
    {
        for (auto & [resource_name, resource] : resource_manager->resources)
            resource->updateNode(NodeInfo(workload_entity, resource_name), NodeInfo(new_entity, resource_name));
        workload_entity = new_entity;
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error in IOResourceManager: {}",
            getCurrentExceptionMessage(/* with_stacktrace = */ true));
    }
}

String IOResourceManager::Workload::getParent() const
{
    return assert_cast<ASTCreateWorkloadQuery *>(workload_entity.get())->getWorkloadParent();
}

IOResourceManager::IOResourceManager(IWorkloadEntityStorage & storage_)
    : storage(storage_)
    , log{getLogger("IOResourceManager")}
{
    subscription = storage.getAllEntitiesAndSubscribe(
        [this] (const std::vector<IWorkloadEntityStorage::Event> & events)
        {
            for (const auto & [entity_type, entity_name, entity] : events)
            {
                switch (entity_type)
                {
                    case WorkloadEntityType::Workload:
                    {
                        if (entity)
                            createOrUpdateWorkload(entity_name, entity);
                        else
                            deleteWorkload(entity_name);
                        break;
                    }
                    case WorkloadEntityType::Resource:
                    {
                        if (entity)
                            createOrUpdateResource(entity_name, entity);
                        else
                            deleteResource(entity_name);
                        break;
                    }
                    case WorkloadEntityType::MAX: break;
                }
            }
        });
}

IOResourceManager::~IOResourceManager()
{
    subscription.reset();
    resources.clear();
    workloads.clear();
}

void IOResourceManager::updateConfiguration(const Poco::Util::AbstractConfiguration &)
{
    // No-op
}

void IOResourceManager::createOrUpdateWorkload(const String & workload_name, const ASTPtr & ast)
{
    std::unique_lock lock{mutex};
    if (auto workload_iter = workloads.find(workload_name); workload_iter != workloads.end())
        workload_iter->second->updateWorkload(ast);
    else
        workloads.emplace(workload_name, std::make_shared<Workload>(this, ast));
}

void IOResourceManager::deleteWorkload(const String & workload_name)
{
    std::unique_lock lock{mutex};
    if (auto workload_iter = workloads.find(workload_name); workload_iter != workloads.end())
    {
        // Note that we rely of the fact that workload entity storage will not drop workload that is used as a parent
        workloads.erase(workload_iter);
    }
    else // Workload to be deleted does not exist -- do nothing, throwing exceptions from a subscription is pointless
        LOG_ERROR(log, "Delete workload that doesn't exist: {}", workload_name);
}

void IOResourceManager::createOrUpdateResource(const String & resource_name, const ASTPtr & ast)
{
    std::unique_lock lock{mutex};
    if (auto resource_iter = resources.find(resource_name); resource_iter != resources.end())
        resource_iter->second->updateResource(ast);
    else
    {
        // Add all workloads into the new resource
        auto resource = std::make_shared<Resource>(ast);
        for (Workload * workload : topologicallySortedWorkloads())
            resource->createNode(NodeInfo(workload->workload_entity, resource_name));

        // Attach the resource
        resources.emplace(resource_name, resource);
    }
}

void IOResourceManager::deleteResource(const String & resource_name)
{
    std::unique_lock lock{mutex};
    if (auto resource_iter = resources.find(resource_name); resource_iter != resources.end())
    {
        resources.erase(resource_iter);
    }
    else // Resource to be deleted does not exist -- do nothing, throwing exceptions from a subscription is pointless
        LOG_ERROR(log, "Delete resource that doesn't exist: {}", resource_name);
}

IOResourceManager::Classifier::~Classifier()
{
    // Detach classifier from all resources in parallel (executed in every scheduler thread)
    std::vector<std::future<void>> futures;
    {
        std::unique_lock lock{mutex};
        futures.reserve(attachments.size());
        for (auto & [resource_name, attachment] : attachments)
        {
            futures.emplace_back(attachment.resource->detachClassifier(std::move(attachment.version)));
            attachment.link.reset(); // Just in case because it is not valid any longer
        }
    }

    // Wait for all tasks to finish (to avoid races in case of exceptions)
    for (auto & future : futures)
        future.wait();

    // There should not be any exceptions because it just destruct few objects, but let's rethrow just in case
    for (auto & future : futures)
        future.get();

    // This unreferences and probably destroys `Resource` objects.
    // NOTE: We cannot do it in the scheduler threads (because thread cannot join itself).
    attachments.clear();
}

std::future<void> IOResourceManager::Resource::detachClassifier(VersionPtr && version)
{
    auto detach_promise = std::make_shared<std::promise<void>>(); // event queue task is std::function, which requires copy semanticss
    auto future = detach_promise->get_future();
    scheduler.event_queue->enqueue([detached_version = std::move(version), promise = std::move(detach_promise)] mutable
    {
        try
        {
            // Unreferences and probably destroys the version and scheduler nodes it owns.
            // The main reason from moving destruction into the scheduler thread is to
            // free memory in the same thread it was allocated to avoid memtrackers drift.
            detached_version.reset();
            promise->set_value();
        }
        catch (...)
        {
            promise->set_exception(std::current_exception());
        }
    });
    return future;
}

bool IOResourceManager::Classifier::has(const String & resource_name)
{
    std::unique_lock lock{mutex};
    return attachments.contains(resource_name);
}

ResourceLink IOResourceManager::Classifier::get(const String & resource_name)
{
    std::unique_lock lock{mutex};
    if (auto iter = attachments.find(resource_name); iter != attachments.end())
        return iter->second.link;
    else
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Access denied to resource '{}'", resource_name);
}

void IOResourceManager::Classifier::attach(const ResourcePtr & resource, const VersionPtr & version, ResourceLink link)
{
    std::unique_lock lock{mutex};
    chassert(!attachments.contains(resource->getName()));
    attachments[resource->getName()] = Attachment{.resource = resource, .version = version, .link = link};
}

void IOResourceManager::Resource::updateResource(const ASTPtr & new_resource_entity)
{
    chassert(getEntityName(new_resource_entity) == resource_name);
    resource_entity = new_resource_entity;
}

std::future<void> IOResourceManager::Resource::attachClassifier(Classifier & classifier, const String & workload_name)
{
    auto attach_promise = std::make_shared<std::promise<void>>(); // event queue task is std::function, which requires copy semantics
    auto future = attach_promise->get_future();
    scheduler.event_queue->enqueue([&, this, promise = std::move(attach_promise)]
    {
        try
        {
            if (auto iter = node_for_workload.find(workload_name); iter != node_for_workload.end())
            {
                auto queue = iter->second->getQueue();
                if (!queue)
                    throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Unable to use workload '{}' that have children for resource '{}'",
                        workload_name, resource_name);
                classifier.attach(shared_from_this(), current_version, ResourceLink{.queue = queue.get()});
            }
            else
            {
                // This resource does not have specified workload. It is either unknown or managed by another resource manager.
                // We leave this resource not attached to the classifier. Access denied will be thrown later on `classifier->get(resource_name)`
            }
            promise->set_value();
        }
        catch (...)
        {
            promise->set_exception(std::current_exception());
        }
    });
    return future;
}

bool IOResourceManager::hasResource(const String & resource_name) const
{
    std::unique_lock lock{mutex};
    return resources.contains(resource_name);
}

ClassifierPtr IOResourceManager::acquire(const String & workload_name)
{
    auto classifier = std::make_shared<Classifier>();

    // Attach classifier to all resources in parallel (executed in every scheduler thread)
    std::vector<std::future<void>> futures;
    {
        std::unique_lock lock{mutex};
        futures.reserve(resources.size());
        for (auto & [resource_name, resource] : resources)
            futures.emplace_back(resource->attachClassifier(*classifier, workload_name));
    }

    // Wait for all tasks to finish (to avoid races in case of exceptions)
    for (auto & future : futures)
        future.wait();

    // Rethrow exceptions if any
    for (auto & future : futures)
        future.get();

    return classifier;
}

void IOResourceManager::Resource::forEachResourceNode(IResourceManager::VisitorFunc & visitor)
{
    executeInSchedulerThread([&, this]
    {
        for (auto & [path, node] : node_for_workload)
        {
            node->forEachSchedulerNode([&] (ISchedulerNode * scheduler_node)
            {
                visitor(resource_name, scheduler_node->getPath(), scheduler_node);
            });
        }
    });
}

void IOResourceManager::forEachNode(IResourceManager::VisitorFunc visitor)
{
    // Copy resource to avoid holding mutex for a long time
    std::unordered_map<String, ResourcePtr> resources_copy;
    {
        std::unique_lock lock{mutex};
        resources_copy = resources;
    }

    /// Run tasks one by one to avoid concurrent calls to visitor
    for (auto & [resource_name, resource] : resources_copy)
        resource->forEachResourceNode(visitor);
}

void IOResourceManager::topologicallySortedWorkloadsImpl(Workload * workload, std::unordered_set<Workload *> & visited, std::vector<Workload *> & sorted_workloads)
{
    if (visited.contains(workload))
        return;
    visited.insert(workload);

    // Recurse into parent (if any)
    String parent = workload->getParent();
    if (!parent.empty())
    {
        auto parent_iter = workloads.find(parent);
        chassert(parent_iter != workloads.end()); // validations check that all parents exist
        topologicallySortedWorkloadsImpl(parent_iter->second.get(), visited, sorted_workloads);
    }

    sorted_workloads.push_back(workload);
}

std::vector<IOResourceManager::Workload *> IOResourceManager::topologicallySortedWorkloads()
{
    std::vector<Workload *> sorted_workloads;
    std::unordered_set<Workload *> visited;
    for (auto & [workload_name, workload] : workloads)
        topologicallySortedWorkloadsImpl(workload.get(), visited, sorted_workloads);
    return sorted_workloads;
}

}
