#include <Common/Scheduler/WorkloadResourceManager.h>

#include <Common/Scheduler/Nodes/SpaceShared/SpaceSharedScheduler.h>
#include <Common/Scheduler/Nodes/TimeShared/TimeSharedScheduler.h>
#include <Common/Scheduler/Nodes/WorkloadNode.h>
#include <Common/Scheduler/Debug.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Name of the workload synthesized when `implicit_default_workload` is enabled and the operator
    /// declared none. It matches the default value of the `workload` query setting, so a query that
    /// leaves `workload` unset resolves to it.
    constexpr std::string_view DEFAULT_WORKLOAD_NAME = "default";

    String getEntityName(const ASTPtr & ast)
    {
        if (auto * create = typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
            return create->getWorkloadName();
        if (auto * create = typeid_cast<ASTCreateResourceQuery *>(ast.get()))
            return create->getResourceName();
        return "unknown-workload-entity";
    }

    CostUnit getResourceUnit(const ASTPtr & ast)
    {
        // CPU resource must have exactly one access mode specified
        if (auto * create = typeid_cast<ASTCreateResourceQuery *>(ast.get()))
            return create->unit;
        return CostUnit::IOByte;
    }

    /// Builds a `CREATE RESOURCE` AST for a server-synthesized (implicit) resource with the given
    /// access modes. It is used only in-memory (never parsed from SQL, never persisted), matching the
    /// shape the parser produces so the rest of the manager treats it like any other resource.
    ASTPtr makeImplicitResourceAST(const String & resource_name, const std::vector<ResourceAccessMode> & modes)
    {
        auto query = make_intrusive<ASTCreateResourceQuery>();
        ASTPtr name_ast = make_intrusive<ASTIdentifier>(resource_name);
        query->resource_name = name_ast;
        query->children.push_back(name_ast);
        for (auto mode : modes)
            query->operations.push_back(ASTCreateResourceQuery::Operation{.mode = mode, .disk = std::nullopt});
        query->unit = query->operations.empty() ? CostUnit::IOByte : query->operations.front().unit();
        return query;
    }

    /// Builds a `CREATE WORKLOAD` AST for a server-synthesized (implicit) parentless workload with the
    /// given name. It is used only in-memory (never parsed from SQL, never persisted), matching the
    /// shape the parser produces so the rest of the manager treats it like any other workload.
    ASTPtr makeImplicitWorkloadAST(const String & workload_name)
    {
        auto query = make_intrusive<ASTCreateWorkloadQuery>();
        ASTPtr name_ast = make_intrusive<ASTIdentifier>(workload_name);
        query->workload_name = name_ast;
        query->children.push_back(name_ast);
        // No parent: the workload attaches directly under the implicit root.
        return query;
    }
}

WorkloadResourceManager::NodeInfo::NodeInfo(CostUnit unit_, const ASTPtr & ast, const String & resource_name)
{
    auto * create = assert_cast<ASTCreateWorkloadQuery *>(ast.get());
    name = create->getWorkloadName();
    parent = create->getWorkloadParent();
    unit = unit_;
    // We ignore unknown settings here for forward-compatibility.
    // There is no way to report error at this point other than stop server.
    settings.initFromChanges(create->changes, resource_name, /*throw_on_unknown_setting=*/ false);
}

WorkloadResourceManager::Resource::Resource(const ASTPtr & resource_entity_)
    : resource_entity(resource_entity_)
    , resource_name(getEntityName(resource_entity))
    , unit(getResourceUnit(resource_entity_))
{
    switch (getSharingMode(unit))
    {
        case SharingMode::TimeShared:
        {
            setup<TimeSharedWorkloadNode, TimeSharedScheduler>();
            break;
        }
        case SharingMode::SpaceShared:
        {
            setup<SpaceSharedWorkloadNode, SpaceSharedScheduler>();
            break;
        }
    }
}

WorkloadResourceManager::Resource::~Resource()
{
    stop(scheduler);
}

void WorkloadResourceManager::Resource::createNode(const NodeInfo & info)
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

    if (!node_for_workload.contains(info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parent node '{}' for creating workload '{}' does not exist in resource '{}'",
            info.parent, info.name, resource_name);

    executeInSchedulerThread([&, this]
    {
        auto node_pair = make_workload_node(scheduler->event_queue, info);
        const WorkloadNodePtr & workload_node = node_pair.first;
        // A parentless workload has parent == "", which maps to the implicit root in node_for_workload.
        node_for_workload[info.parent]->attachWorkloadChild(workload_node);
        node_for_workload[info.name] = workload_node;

        updateCurrentVersion();
    });
}

void WorkloadResourceManager::Resource::deleteNode(const NodeInfo & info)
{
    if (!node_for_workload.contains(info.name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node for removing workload '{}' does not exist in resource '{}'",
            info.name, resource_name);

    if (!node_for_workload.contains(info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parent node '{}' for removing workload '{}' does not exist in resource '{}'",
            info.parent, info.name, resource_name);

    auto node = node_for_workload[info.name];

    if (node->hasWorkloadChildren())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Removing workload '{}' with children in resource '{}'",
        info.name, resource_name);

    executeInSchedulerThread([&, n = std::move(node)]() mutable
    {
        // A parentless workload has parent == "", which maps to the implicit root in node_for_workload.
        node_for_workload[info.parent]->detachWorkloadChild(n);

        node_for_workload.erase(info.name);

        updateCurrentVersion();

        // Note: `n` must be explicitly destroyed here, in the scheduler thread,
        // to avoid a data race between the destructor and the scheduler thread
        // that may still process activations for this node.
        // Without this explicit reset, `n` (a captured lambda member) would be
        // destroyed when the lambda itself is destroyed — which happens in the
        // caller's thread after `executeInSchedulerThread` returns, not here.
        n.reset();
    });
}

void WorkloadResourceManager::Resource::updateNode(const NodeInfo & old_info, const NodeInfo & new_info)
{
    SCHED_DBG("WorkloadResourceManager -- updateNode(resource={}, workload={})", resource_name, old_info.name);
    if (old_info.name != new_info.name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Updating a name of workload '{}' to '{}' is not allowed in resource '{}'",
            old_info.name, new_info.name, resource_name);

    if (!node_for_workload.contains(old_info.name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node for updating workload '{}' does not exist in resource '{}'",
            old_info.name, resource_name);

    if (!node_for_workload.contains(old_info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Old parent node '{}' for updating workload '{}' does not exist in resource '{}'",
            old_info.parent, old_info.name, resource_name);

    if (!node_for_workload.contains(new_info.parent))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "New parent node '{}' for updating workload '{}' does not exist in resource '{}'",
            new_info.parent, new_info.name, resource_name);

    executeInSchedulerThread([&, this]
    {
        SCHED_DBG("WorkloadResourceManager -- [begin] updateNode(resource={}, workload={})", resource_name, old_info.name);
        auto node = node_for_workload[old_info.name];
        bool detached = false;
        if (IWorkloadNode::updateRequiresDetach(
            old_info.parent,
            new_info.parent,
            old_info.settings,
            new_info.settings,
            getSharingMode(getUnit())))
        {
            // Detach here and reattach below so the workload is re-positioned among its siblings for
            // the new parent/priority/precedence (parent "" maps to the implicit root).
            node_for_workload[old_info.parent]->detachWorkloadChild(node);
            detached = true;
        }

        node->updateSchedulingSettings(new_info.settings);

        if (detached)
            node_for_workload[new_info.parent]->attachWorkloadChild(node);
        updateCurrentVersion();
        SCHED_DBG("WorkloadResourceManager -- [end] updateNode(resource={}, workload={})", resource_name, old_info.name);
    });
}

void WorkloadResourceManager::Resource::updateCurrentVersion()
{
    auto previous_version = current_version;

    // Create a full list of constraints and queues in the current hierarchy (walk from the implicit
    // root, which owns every workload subtree of this resource).
    current_version = std::make_shared<Version>();
    if (auto root = implicitRoot())
        root->addRawPointerNodes(current_version->nodes);

    // See details in version control section of description in WorkloadResourceManager.h
    if (previous_version)
    {
        previous_version->newer_version = current_version;
        previous_version.reset(); // Destroys previous version nodes if there are no classifiers referencing it
    }
}

WorkloadResourceManager::Workload::Workload(WorkloadResourceManager * resource_manager_, const ASTPtr & workload_entity_)
    : resource_manager(resource_manager_)
    , workload_entity(workload_entity_)
{
    try
    {
        for (auto & [resource_name, resource] : resource_manager->resources)
            resource->createNode(NodeInfo(resource->getUnit(), workload_entity, resource_name));
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error in WorkloadResourceManager: {}",
            getCurrentExceptionMessage(/* with_stacktrace = */ true));
    }
}

WorkloadResourceManager::Workload::~Workload()
{
    try
    {
        for (auto & [resource_name, resource] : resource_manager->resources)
            resource->deleteNode(NodeInfo(resource->getUnit(), workload_entity, resource_name));
    }
    catch (...)
    {
        tryLogCurrentException("Workload");
        chassert(false);
    }
}

void WorkloadResourceManager::Workload::updateWorkload(const ASTPtr & new_entity)
{
    try
    {
        for (auto & [resource_name, resource] : resource_manager->resources)
            resource->updateNode(NodeInfo(resource->getUnit(), workload_entity, resource_name), NodeInfo(resource->getUnit(), new_entity, resource_name));
        workload_entity = new_entity;
    }
    catch (...)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error in WorkloadResourceManager: {}",
            getCurrentExceptionMessage(/* with_stacktrace = */ true));
    }
}

String WorkloadResourceManager::Workload::getParent() const
{
    return assert_cast<ASTCreateWorkloadQuery *>(workload_entity.get())->getWorkloadParent();
}

WorkloadResourceManager::WorkloadResourceManager(std::shared_ptr<IWorkloadEntityStorage> storage_)
    : storage(std::move(storage_))
    , log{getLogger("WorkloadResourceManager")}
{
    subscription = storage->getAllEntitiesAndSubscribe(
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

WorkloadResourceManager::~WorkloadResourceManager()
{
    subscription.reset();
    resources.clear();
    workloads.clear();
}

void WorkloadResourceManager::createOrUpdateWorkload(const String & workload_name, const ASTPtr & ast)
{
    std::unique_lock lock{mutex};
    if (auto workload_iter = workloads.find(workload_name); workload_iter != workloads.end())
        workload_iter->second->updateWorkload(ast);
    else
        workloads.emplace(workload_name, std::make_shared<Workload>(this, ast));

    // If the operator now declares `default` explicitly, it takes ownership of the entry, so the
    // synthesized one is not removed when `implicit_default_workload` is later disabled.
    if (workload_name == DEFAULT_WORKLOAD_NAME)
        default_workload_synthesized = false;
}

void WorkloadResourceManager::deleteWorkload(const String & workload_name)
{
    std::unique_lock lock{mutex};
    if (auto workload_iter = workloads.find(workload_name); workload_iter != workloads.end())
    {
        // Note that we rely of the fact that workload entity storage will not drop workload that is used as a parent
        workloads.erase(workload_iter);
    }
    else // Workload to be deleted does not exist -- do nothing, throwing exceptions from a subscription is pointless
        LOG_ERROR(log, "Delete workload that doesn't exist: {}", workload_name);

    // If the operator dropped an explicit `default` while `implicit_default_workload` is on, restore
    // the synthesized one so `workload='default'` keeps resolving.
    if (workload_name == DEFAULT_WORKLOAD_NAME)
        applyImplicitDefaultWorkloadLocked();
}

void WorkloadResourceManager::createOrUpdateResource(const String & resource_name, const ASTPtr & ast)
{
    std::unique_lock lock{mutex};
    if (auto resource_iter = resources.find(resource_name); resource_iter != resources.end())
        resource_iter->second->updateResource(ast);
    else
    {
        // Add all workloads into the new resource
        auto resource = std::make_shared<Resource>(ast);
        for (Workload * workload : topologicallySortedWorkloads())
            resource->createNode(NodeInfo(resource->getUnit(), workload->workload_entity, resource_name));

        // Attach the resource
        resources.emplace(resource_name, resource);
    }

    // Re-apply the latest server-wide limits after any resource create OR replace: a newly created
    // operator resource must take over the implicit one, and a CREATE OR REPLACE that changes the
    // relevant role must re-derive the implicit resource and root accordingly.
    applyServerLimitsLocked();
}

void WorkloadResourceManager::deleteResource(const String & resource_name)
{
    std::unique_lock lock{mutex};
    if (auto resource_iter = resources.find(resource_name); resource_iter != resources.end())
    {
        resources.erase(resource_iter);
        // Re-apply the latest server-wide limits: if the dropped resource was the operator CPU/memory
        // resource while the feature is enabled, the implicit server-limit resource must be recreated
        // so the budget keeps applying without waiting for the next config reload.
        applyServerLimitsLocked();
    }
    else // Resource to be deleted does not exist -- do nothing, throwing exceptions from a subscription is pointless
        LOG_ERROR(log, "Delete resource that doesn't exist: {}", resource_name);
}

WorkloadResourceManager::ResourcePtr WorkloadResourceManager::createImplicitResourceLocked(const String & resource_name, const ASTPtr & ast)
{
    auto resource = std::make_shared<Resource>(ast);
    for (Workload * workload : topologicallySortedWorkloads())
        resource->createNode(NodeInfo(resource->getUnit(), workload->workload_entity, resource_name));
    resources.emplace(resource_name, resource);
    return resource;
}

void WorkloadResourceManager::updateServerLimits(const ServerResourceLimits & limits)
{
    std::unique_lock lock{mutex};
    current_limits = limits;
    applyServerLimitsLocked();
}

void WorkloadResourceManager::applyServerLimitsLocked()
{
    // The implicit `default` workload is gated independently of the resource-limit settings.
    applyImplicitDefaultWorkloadLocked();

    const bool any_enabled = current_limits.respect_cpu_limit || current_limits.respect_memory_limit;

    // Keep the default (unlimited) behavior without touching any scheduler node while nothing is
    // requested and nothing has ever been applied.
    if (!any_enabled && !server_limits_applied)
        return;

    // CPU concurrency budget on a combined `MASTER THREAD, WORKER THREAD` resource.
    applyResourceLimitLocked(
        CostUnit::CPUNanosecond,
        String(IMPLICIT_CPU_RESOURCE_NAME),
        {ResourceAccessMode::MasterThread, ResourceAccessMode::WorkerThread},
        current_limits.respect_cpu_limit,
        current_limits.cpu_slots,
        [](WorkloadSettings & s, Int64 v) { s.max_concurrent_threads = v; });

    // Memory reservation admission budget on a `MEMORY RESERVATION` resource.
    applyResourceLimitLocked(
        CostUnit::MemoryByte,
        String(IMPLICIT_MEMORY_RESOURCE_NAME),
        {ResourceAccessMode::MemoryReservation},
        current_limits.respect_memory_limit,
        current_limits.memory_bytes,
        [](WorkloadSettings & s, Int64 v) { s.max_memory = v; });

    server_limits_applied = any_enabled;
}

void WorkloadResourceManager::applyImplicitDefaultWorkloadLocked()
{
    const String default_name(DEFAULT_WORKLOAD_NAME);
    if (current_limits.implicit_default_workload)
    {
        // Synthesize a `default` workload under the implicit root if the operator declared none, so a
        // query with `workload='default'` resolves to a real workload node. Not persisted through the
        // entity storage, analogous to the implicit resources.
        if (!workloads.contains(default_name))
        {
            workloads.emplace(default_name, std::make_shared<Workload>(this, makeImplicitWorkloadAST(default_name)));
            default_workload_synthesized = true;
        }
    }
    else if (default_workload_synthesized)
    {
        // Feature disabled: drop the synthesized workload; an operator-declared one is never removed.
        workloads.erase(default_name);
        default_workload_synthesized = false;
    }
}

void WorkloadResourceManager::applyResourceLimitLocked(
    CostUnit unit,
    const String & implicit_name,
    const std::vector<ResourceAccessMode> & implicit_modes,
    bool enabled,
    Int64 effective_limit,
    const std::function<void(WorkloadSettings &, Int64)> & set_limit_field)
{
    // Snapshot resources of this unit before mutating the map (creating the implicit resource inserts
    // into `resources`, which would otherwise invalidate an in-progress iteration).
    std::vector<ResourcePtr> operator_resources;
    ResourcePtr implicit;
    for (auto & [name, resource] : resources)
    {
        if (resource->getUnit() != unit)
            continue;
        if (name == implicit_name)
            implicit = resource;
        else
            operator_resources.push_back(resource);
    }

    // The feature mirrors one server-wide budget onto a single implicit-root constraint, so it
    // supports at most one operator-declared resource of this unit (or none, in which case a combined
    // implicit resource is created). More than one operator resource of the same unit (for CPU, e.g.
    // separate MASTER and WORKER resources) cannot carry one shared budget without double-counting it,
    // so the setting has no effect for that unsupported configuration: roots stay unlimited and no
    // implicit resource is created.
    const bool supported = operator_resources.empty()
        || (operator_resources.size() == 1 && operator_resources.front()->coversAllModes(implicit_modes));
    if (enabled && supported)
    {
        // An operator-declared resource takes precedence: drop the auto-created one if both exist.
        if (!operator_resources.empty())
        {
            if (implicit)
            {
                resources.erase(implicit_name);
                implicit.reset();
            }
        }
        else if (!implicit)
        {
            implicit = createImplicitResourceLocked(implicit_name, makeImplicitResourceAST(implicit_name, implicit_modes));
        }

        WorkloadSettings root_settings;
        set_limit_field(root_settings, effective_limit);
        if (implicit)
            implicit->setImplicitRootLimit(root_settings);
        for (auto & resource : operator_resources)
            resource->setImplicitRootLimit(root_settings);
    }
    else
    {
        // Reset any operator resource's root to unlimited in place; never remove an operator resource.
        WorkloadSettings unlimited_root_settings;
        for (auto & resource : operator_resources)
            resource->setImplicitRootLimit(unlimited_root_settings);
        // Remove the auto-created implicit resource; its nodes drain via the version machinery once no
        // classifier references them any longer.
        if (implicit)
            resources.erase(implicit_name);
    }
}

WorkloadResourceManager::Classifier::Classifier(const ClassifierSettings & settings_)
    : settings(settings_)
{
}

WorkloadResourceManager::Classifier::~Classifier()
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

std::future<void> WorkloadResourceManager::Resource::detachClassifier(VersionPtr && version)
{
    auto detach_promise = std::make_shared<std::promise<void>>(); // event queue task is std::function, which requires copy semanticss
    auto future = detach_promise->get_future();
    scheduler->event_queue.enqueue([detached_version = std::move(version), promise = std::move(detach_promise)] mutable
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

bool WorkloadResourceManager::Classifier::has(const String & resource_name)
{
    std::unique_lock lock{mutex};
    return attachments.contains(resource_name);
}

ResourceLink WorkloadResourceManager::Classifier::get(const String & resource_name)
{
    std::unique_lock lock{mutex};
    if (auto iter = attachments.find(resource_name); iter != attachments.end())
    {
        return iter->second.link;
    }
    else
    {
        if (settings.throw_on_unknown_workload)
            throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Could not access resource '{}'. Please check `throw_on_unknown_workload` setting", resource_name);
        else
            return ResourceLink{}; // unlimited access
    }
}

WorkloadSettings WorkloadResourceManager::Classifier::getWorkloadSettings(const String & resource_name) const
{
    std::unique_lock lock{mutex};
    auto iter = attachments.find(resource_name);
    if (iter != attachments.end())
    {
        // Extract settings from the attached resource
        return iter->second.settings;
    }
    return {};
}

void WorkloadResourceManager::Classifier::attach(const ResourcePtr & resource, const VersionPtr & version, IWorkloadNode & node)
{
    std::unique_lock lock{mutex};
    chassert(!attachments.contains(resource->getName()));
    attachments[resource->getName()] = Attachment{.resource = resource, .version = version, .link = node.getLink(), .settings = node.getSettings()};
}

void WorkloadResourceManager::Resource::updateResource(const ASTPtr & new_resource_entity)
{
    chassert(getEntityName(new_resource_entity) == resource_name);
    chassert(getResourceUnit(new_resource_entity) == unit); // resource unit cannot be changed
    resource_entity = new_resource_entity;
}

bool WorkloadResourceManager::Resource::coversAllModes(const std::vector<ResourceAccessMode> & required) const
{
    const auto * create = assert_cast<const ASTCreateResourceQuery *>(resource_entity.get());
    for (auto mode : required)
    {
        bool found = false;
        for (const auto & operation : create->operations)
        {
            if (operation.mode == mode)
            {
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }
    return true;
}

void WorkloadResourceManager::Resource::setImplicitRootLimit(const WorkloadSettings & root_settings)
{
    executeInSchedulerThread([&, this]
    {
        // The implicit root is an internal node with no explicit parent/priority, so its update never
        // requires detach (see `updateRequiresDetach`): a `max_*` change is an in-place numeric update
        // that creates, updates, or removes the root constraint without reparenting.
        if (auto root = implicitRoot())
        {
            root->updateSchedulingSettings(root_settings);
            updateCurrentVersion();
        }
    });
}

std::future<void> WorkloadResourceManager::Resource::attachClassifier(Classifier & classifier, const String & workload_name)
{
    auto attach_promise = std::make_shared<std::promise<void>>(); // event queue task is std::function, which requires copy semantics
    auto future = attach_promise->get_future();
    scheduler->event_queue.enqueue([&, this, promise = std::move(attach_promise)]
    {
        try
        {
            // The implicit root lives under the empty-string key; it is internal, not a workload a
            // query can be classified into, so an empty workload name is treated as unknown.
            if (auto iter = node_for_workload.find(workload_name); !workload_name.empty() && iter != node_for_workload.end())
                classifier.attach(shared_from_this(), current_version, *iter->second);
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

bool WorkloadResourceManager::hasResource(const String & resource_name) const
{
    std::unique_lock lock{mutex};
    return resources.contains(resource_name);
}

ClassifierPtr WorkloadResourceManager::acquire(const String & workload_name, const ClassifierSettings & settings)
{
    auto classifier = std::make_shared<Classifier>(settings);

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

void WorkloadResourceManager::Resource::forEachResourceNode(IResourceManager::VisitorFunc & visitor)
{
    executeInSchedulerThread([&, this]
    {
        // node_for_workload includes the implicit root under the empty-string key, so this also
        // exposes the implicit root and the inter-root scheduling nodes it holds in system.scheduler.
        for (auto & [path, node] : node_for_workload)
        {
            node->forEachSchedulerNode([&] (ISchedulerNode * scheduler_node)
            {
                visitor(resource_name, scheduler_node->getPath(), scheduler_node);
            });
        }
    });
}

void WorkloadResourceManager::forEachNode(IResourceManager::VisitorFunc visitor)
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

void WorkloadResourceManager::topologicallySortedWorkloadsImpl(Workload * workload, std::unordered_set<Workload *> & visited, std::vector<Workload *> & sorted_workloads)
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

std::vector<WorkloadResourceManager::Workload *> WorkloadResourceManager::topologicallySortedWorkloads()
{
    std::vector<Workload *> sorted_workloads;
    std::unordered_set<Workload *> visited;
    for (auto & [workload_name, workload] : workloads)
        topologicallySortedWorkloadsImpl(workload.get(), visited, sorted_workloads);
    return sorted_workloads;
}

}
