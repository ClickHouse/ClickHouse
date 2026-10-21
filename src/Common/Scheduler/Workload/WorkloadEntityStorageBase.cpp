#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>

#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/logger_useful.h>
#include <Common/StringUtils.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadEntity.h>
#include <Parsers/parseQuery.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Common/escapeForFileName.h>

#include <filesystem>

#include <boost/range/algorithm/copy.hpp>

#include <mutex>
#include <queue>
#include <unordered_set>
#include <variant>

namespace DB
{

namespace fs = std::filesystem;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Removes details from a CREATE query to be used as workload entity definition
ASTPtr normalizeCreateWorkloadEntityQuery(const IAST & create_query)
{
    auto ptr = create_query.clone();
    if (auto * res = typeid_cast<ASTCreateWorkloadQuery *>(ptr.get()))
    {
        res->if_not_exists = false;
        res->or_replace = false;
    }
    if (auto * res = typeid_cast<ASTCreateResourceQuery *>(ptr.get()))
    {
        res->if_not_exists = false;
        res->or_replace = false;
    }
    return ptr;
}

/// Returns the kind and the name of a workload entity `ptr`
WorkloadEntityKey getEntityKey(const ASTPtr & ptr)
{
    if (auto * res = typeid_cast<ASTCreateWorkloadQuery *>(ptr.get()))
        return {WorkloadEntityType::Workload, res->getWorkloadName()};
    if (auto * res = typeid_cast<ASTCreateResourceQuery *>(ptr.get()))
        return {WorkloadEntityType::Resource, res->getResourceName()};
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid workload entity query: {}", ptr->getID());
}

/// Returns `workload` or `resource` for messages
std::string_view getKindName(WorkloadEntityType type)
{
    return type == WorkloadEntityType::Workload ? "workload" : "resource";
}

bool entityEquals(const ASTPtr & lhs, const ASTPtr & rhs)
{
    if (auto * a = typeid_cast<ASTCreateWorkloadQuery *>(lhs.get()))
    {
        if (auto * b = typeid_cast<ASTCreateWorkloadQuery *>(rhs.get()))
        {
            return std::forward_as_tuple(a->getWorkloadName(), a->getWorkloadParent(), a->changes)
                == std::forward_as_tuple(b->getWorkloadName(), b->getWorkloadParent(), b->changes);
        }
    }
    if (auto * a = typeid_cast<ASTCreateResourceQuery *>(lhs.get()))
    {
        if (auto * b = typeid_cast<ASTCreateResourceQuery *>(rhs.get()))
            return std::forward_as_tuple(a->getResourceName(), a->operations)
                == std::forward_as_tuple(b->getResourceName(), b->operations);
    }
    return false;
}

/// True if both entities are resources with different cost units
bool resourceUnitChanged(const ASTPtr & before, const ASTPtr & after)
{
    auto * before_resource = typeid_cast<ASTCreateResourceQuery *>(before.get());
    auto * after_resource = typeid_cast<ASTCreateResourceQuery *>(after.get());
    return before_resource && after_resource && before_resource->unit != after_resource->unit;
}

/// Throws if `entity_name` cannot be used for a user entity (SQL, configuration, Keeper or disk).
/// The empty name denotes the root workload; the names of implicit resources are reserved.
void checkUserEntityName(const String & entity_name)
{
    if (entity_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity name should not be empty.");
    if (entity_name == IMPLICIT_CPU_RESOURCE_NAME || entity_name == IMPLICIT_MEMORY_RESOURCE_NAME)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Workload entity name '{}' is reserved for an implicit resource and cannot be used", entity_name);
}

/// A claim key (see IWorkloadEntityStorage.h): an entity or an access-mode role of a resource
using ClaimKey = std::variant<WorkloadEntityKey, ResourceAccessMode>;

String roleToString(ResourceAccessMode mode)
{
    switch (mode)
    {
        case ResourceAccessMode::DiskRead: return "READ DISK";
        case ResourceAccessMode::DiskWrite: return "WRITE DISK";
        case ResourceAccessMode::MasterThread: return "MASTER THREAD";
        case ResourceAccessMode::WorkerThread: return "WORKER THREAD";
        case ResourceAccessMode::Query: return "QUERY";
        case ResourceAccessMode::MemoryReservation: return "MEMORY RESERVATION";
    }
}

/// Returns all claim keys of an entity
std::vector<ClaimKey> getClaimKeys(const WorkloadEntityKey & key, const ASTPtr & entity)
{
    std::vector<ClaimKey> keys{ClaimKey{key}};
    if (auto * resource = typeid_cast<ASTCreateResourceQuery *>(entity.get()))
    {
        for (const auto & operation : resource->operations)
        {
            switch (operation.mode)
            {
                case ResourceAccessMode::MasterThread:
                case ResourceAccessMode::WorkerThread:
                case ResourceAccessMode::Query:
                case ResourceAccessMode::MemoryReservation:
                {
                    ClaimKey claim{operation.mode};
                    if (std::find(keys.begin(), keys.end(), claim) == keys.end())
                        keys.push_back(std::move(claim));
                    break;
                }
                case ResourceAccessMode::DiskRead:
                case ResourceAccessMode::DiskWrite:
                    break; // Several resources may serve one disk
            }
        }
    }
    return keys;
}

[[noreturn]] void throwClaimConflict(const ClaimKey & claim, const String & holder)
{
    if (const auto * mode = std::get_if<ResourceAccessMode>(&claim))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second resource for {} is not allowed. Current resource name: '{}'.", roleToString(*mode), holder);
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists", holder);
}

/// Runs a `func` callback for every reference of a workload `source_entity`: to its parent workload and to the
/// resources in its `SETTINGS x = y FOR resource` clauses. A resource has no references.
/// This function is the source of truth defining what references are stored in a workload.
void forEachReference(
    const ASTPtr & source_entity,
    std::function<void(const WorkloadEntityKey & target, const String & source)> func)
{
    if (auto * res = typeid_cast<ASTCreateWorkloadQuery *>(source_entity.get()))
    {
        String parent = res->getWorkloadParent();
        if (!parent.empty())
            func({WorkloadEntityType::Workload, parent}, res->getWorkloadName());

        std::unordered_set<String> resources;
        for (const auto & [name, value, resource] : res->changes)
        {
            if (!resource.empty())
                resources.insert(resource);
        }
        for (const String & resource : resources)
            func({WorkloadEntityType::Resource, resource}, res->getWorkloadName());
    }
}

/// Throws if workloads of `entities` form a parent cycle. Every workload has at most one parent, so a walk up from a
/// workload ends outside `entities`, at a parentless workload, at a workload checked by an earlier walk, or on its own
/// path, which is a cycle. Every workload is walked over once.
void checkParentCycles(const WorkloadEntities & entities)
{
    enum class State : uint8_t { OnPath, Checked };
    std::unordered_map<std::string_view, State> states;
    std::vector<std::string_view> path;
    for (const auto & [key, entity] : entities)
    {
        if (key.type != WorkloadEntityType::Workload)
            continue;
        path.clear();
        String current = key.name;
        while (true)
        {
            if (auto state = states.find(current); state != states.end())
            {
                if (state->second == State::OnPath)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity cycles are not allowed: workload '{}' is on a parent cycle", current);
                break;
            }
            auto it = entities.find({WorkloadEntityType::Workload, current});
            if (it == entities.end())
                break;
            // Views point to the names of keys in `entities`, which outlive `states` and `path`
            states.emplace(it->first.name, State::OnPath);
            path.push_back(it->first.name);
            auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(it->second.get());
            if (!workload->hasParent())
                break;
            current = workload->getWorkloadParent();
        }
        for (std::string_view name : path)
            states[name] = State::Checked;
    }
}

/// Helper for recursive DFS
void topologicallySortedWorkloadsImpl(const String & name, const ASTPtr & ast, const std::unordered_map<String, ASTPtr> & workloads, std::unordered_set<String> & visited, std::vector<std::pair<String, ASTPtr>> & sorted_workloads)
{
    if (visited.contains(name))
        return;
    visited.insert(name);

    // Recurse into parent (if any)
    String parent = typeid_cast<ASTCreateWorkloadQuery *>(ast.get())->getWorkloadParent();
    if (!parent.empty())
    {
        if (auto parent_iter = workloads.find(parent); parent_iter != workloads.end())
            topologicallySortedWorkloadsImpl(parent, parent_iter->second, workloads, visited, sorted_workloads);
        else
        {
            // This is okay because parent workload could be defined outside of the current set of workloads (e.g. in another storage)
        }
    }

    sorted_workloads.emplace_back(name, ast);
}

/// Returns pairs {worload_name, create_workload_ast} in order that respect child-parent relation (parent first, then children)
std::vector<std::pair<String, ASTPtr>> topologicallySortedWorkloads(const std::unordered_map<String, ASTPtr> & workloads)
{
    std::vector<std::pair<String, ASTPtr>> sorted_workloads;
    std::unordered_set<String> visited;
    for (const auto & [name, ast] : workloads)
        topologicallySortedWorkloadsImpl(name, ast, workloads, visited, sorted_workloads);
    return sorted_workloads;
}

using Dependencies = std::map<WorkloadEntityKey, std::set<WorkloadEntityKey>>;

/// Helper for recursive DFS
void topologicallySortedDependenciesImpl(
    const WorkloadEntityKey & key,
    const Dependencies & dependencies,
    std::set<WorkloadEntityKey> & visited,
    std::vector<WorkloadEntityKey> & result)
{
    if (!visited.insert(key).second)
        return;

    if (auto it = dependencies.find(key); it != dependencies.end())
    {
        for (const auto & dep : it->second)
            topologicallySortedDependenciesImpl(dep, dependencies, visited, result);
    }

    result.emplace_back(key);
}

/// Returns nodes in topological order that respect `dependencies` (key is a node, value is set of its dependencies)
std::vector<WorkloadEntityKey> topologicallySortedDependencies(const Dependencies & dependencies)
{
    std::set<WorkloadEntityKey> visited;
    std::vector<WorkloadEntityKey> result;
    for (const auto & [key, _] : dependencies)
        topologicallySortedDependenciesImpl(key, dependencies, visited, result);
    return result;
}

/// Represents a change of a workload entity (WORKLOAD or RESOURCE)
struct EntityChange
{
    WorkloadEntityKey key;
    ASTPtr before; /// Entity before change (CREATE if not set)
    ASTPtr after; /// Entity after change (DROP if not set)

    /// True if the change drops the entity and creates a new one (the cost unit changes)
    bool recreates() const
    {
        return before && after && resourceUnitChanged(before, after);
    }

    std::vector<IWorkloadEntityStorage::Event> toEvents() const
    {
        if (!after)
            return {{key.type, key.name, {}}};
        if (recreates())
            return {{key.type, key.name, {}}, {key.type, key.name, after}};
        return {{key.type, key.name, after}};
    }
};

/// Returns `changes` ordered for execution.
/// Every intemediate state during execution will be consistent (i.e. all references will be valid)
/// NOTE: It does not validate changes, any problem will be detected during execution.
/// NOTE: There will be no error if valid order does not exist.
std::vector<EntityChange> topologicallySortedChanges(const std::vector<EntityChange> & changes)
{
    std::map<WorkloadEntityKey, const EntityChange *> change_by_key;
    for (const auto & change : changes)
        change_by_key[change.key] = &change;

    // Construct references maps (before changes and after changes)
    Dependencies old_sources; // Key is target. Value is set of source entities.
    Dependencies new_targets; // Key is source. Value is set of target entities.
    for (const auto & change : changes)
    {
        if (change.before)
            forEachReference(change.before, [&] (const WorkloadEntityKey & target, const String & source)
            {
                old_sources[target].insert({WorkloadEntityType::Workload, source});
            });
        if (change.after)
            forEachReference(change.after, [&] (const WorkloadEntityKey & target, const String & source)
            {
                new_targets[{WorkloadEntityType::Workload, source}].insert(target);
            });
    }

    // Construct DAG of dependencies between changes. Key is entity. Value is set of entities that should be changed first.
    Dependencies dependencies;
    for (const auto & change : changes)
    {
        dependencies.emplace(change.key, std::set<WorkloadEntityKey>{}); // Make sure we create nodes that have no dependencies
        for (const auto & event : change.toEvents())
        {
            if (!event.entity) // DROP
            {
                // Rule 1: Entity can only be removed after all existing references to it are removed as well.
                for (const auto & source : old_sources[change.key])
                {
                    if (change_by_key.contains(source))
                        dependencies[change.key].insert(source);
                }
            }
            else // CREATE || CREATE OR REPLACE
            {
                // Rule 2: Entity can only be created after all entities it references are created as well.
                for (const auto & target : new_targets[change.key])
                {
                    // If target is creating (or recreating), it should be created first.
                    // (But if target is updating, there is no dependency).
                    if (auto it = change_by_key.find(target); it != change_by_key.end() && (!it->second->before || it->second->recreates()))
                        dependencies[change.key].insert(target);
                }
            }
        }
    }

    std::vector<EntityChange> result;
    for (const auto & key : topologicallySortedDependencies(dependencies))
        result.push_back(*change_by_key[key]);
    return result;
}

}

WorkloadEntityStorageBase::WorkloadEntityStorageBase(ContextPtr global_context_, std::unique_ptr<IWorkloadEntityStorage> next_storage_)
    : handlers(std::make_shared<Handlers>())
    , next_storage(std::move(next_storage_))
    , global_context(std::move(global_context_))
    , log{getLogger("WorkloadEntityStorage")} // could be overridden in derived class
{
    if (next_storage)
    {
        subscription = next_storage->getAllEntitiesAndSubscribe(
            [this] (const std::vector<Event> & inner_tx)
            {
                std::unique_lock lock(mutex);

                // Sync `inherited_entities` with the next storage
                for (const auto & event : inner_tx)
                {
                    if (event.entity)
                        inherited_entities[{event.type, event.name}] = event.entity;
                    else
                        inherited_entities.erase({event.type, event.name});
                }

                // Transaction over the next storage is transformed into one transaction over this storage
                updateVisibleEntities(lock);
            });
    }
}

WorkloadEntityStorageBase::~WorkloadEntityStorageBase()
{
    /// The chain subscription must be dropped before members are destroyed: its handler reads
    /// `log`, which is declared after `subscription` and would die first.
    subscription.reset();
}

ASTPtr WorkloadEntityStorageBase::get(WorkloadEntityType entity_type, const String & entity_name) const
{
    if (auto result = tryGet(entity_type, entity_name))
        return result;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "The workload entity name '{}' is not saved",
        entity_name);
}

ASTPtr WorkloadEntityStorageBase::tryGet(WorkloadEntityType entity_type, const String & entity_name) const
{
    std::lock_guard lock(mutex);

    auto it = visible_entities.find({entity_type, entity_name});
    if (it == visible_entities.end())
        return nullptr;

    return it->second;
}

bool WorkloadEntityStorageBase::has(WorkloadEntityType entity_type, const String & entity_name) const
{
    return tryGet(entity_type, entity_name) != nullptr;
}

String WorkloadEntityStorageBase::getEntitySource(WorkloadEntityType entity_type, const String & entity_name) const
{
    {
        std::lock_guard lock(mutex);
        if (!visible_entities.contains({entity_type, entity_name}))
            return "";
        if (!inherited_entities.contains({entity_type, entity_name}))
            return String(getName());
    }
    // Not under the lock: the next storage may notify this one while holding its own lock
    return next_storage->getEntitySource(entity_type, entity_name);
}

bool WorkloadEntityStorageBase::empty() const
{
    std::lock_guard lock(mutex);
    return visible_entities.empty();
}

void WorkloadEntityStorageBase::loadEntities(const Poco::Util::AbstractConfiguration & config)
{
    if (next_storage)
        next_storage->loadEntities(config);
}

bool WorkloadEntityStorageBase::storeEntity(
    const ContextPtr & current_context,
    WorkloadEntityType entity_type,
    const String & entity_name,
    ASTPtr create_entity_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    checkUserEntityName(entity_name);

    create_entity_query = normalizeCreateWorkloadEntityQuery(*create_entity_query);
    const WorkloadEntityKey key{entity_type, entity_name};
    auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(create_entity_query.get());

    while (true)
    {
        std::unique_lock lock{mutex};

        // An entity of this kind and name may be stored in this layer (possibly shadowed) or in an inner one
        ASTPtr old_entity = findKnownEntity(key, own_entities); // entity to be REPLACED
        if (old_entity)
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists", entity_name);
            if (!replace_if_exists)
                return false;
            if (inherited_entities.contains(key))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "It is not allowed to replace workload entity '{}' that is stored in read-only {} storage", entity_name, next_storage->getName());
        }

        // Check the settings values and throw if something is wrong
        if (workload)
        {
            WorkloadSettings validator;
            validator.initFromChanges(workload->changes);
        }

        // The scheduler hierarchy is built with unit-specific node types
        if (old_entity && resourceUnitChanged(old_entity, create_entity_query))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cost unit of resource '{}' cannot be changed via CREATE OR REPLACE; drop and recreate the resource instead.",
                entity_name);

        checkClaimConflicts(key, create_entity_query);

        // Targets are looked up in own and inherited entities only, so entities of outer layers cannot be referenced
        forEachReference(create_entity_query,
            [&] (const WorkloadEntityKey & target, const String & source)
            {
                if (!findKnownEntity(target, own_entities))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload '{}' references {} '{}' that doesn't exist", source, getKindName(target.type), target.name);

                // Validate that we could parse the settings for specific resource
                if (target.type == WorkloadEntityType::Resource)
                {
                    WorkloadSettings validator;
                    validator.initFromChanges(workload->changes, target.name);
                }

                // Detect reference cycles.
                // The only way to create a cycle is to add an edge that will be a part of a new cycle.
                // We are going to add an edge: `source` -> `target`, so we ensure there is no path back `target` -> `source`.
                if (isIndirectlyReferenced(key, target))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity cycles are not allowed");
            });

        auto result = storeEntityImpl(current_context, entity_type, entity_name, create_entity_query, throw_if_exists, replace_if_exists, settings);
        if (result == OperationResult::Ok)
            own_entities[key] = create_entity_query;

        if (result == OperationResult::Retry)
            continue; // Entities were updated, we need to rerun all the validations

        if (result == OperationResult::Ok)
            updateVisibleEntities(lock);

        return result == OperationResult::Ok;
    }
}

bool WorkloadEntityStorageBase::removeEntity(
    const ContextPtr & current_context,
    WorkloadEntityType entity_type,
    const String & entity_name,
    bool throw_if_not_exists)
{
    const WorkloadEntityKey key{entity_type, entity_name};
    while (true)
    {
        std::unique_lock lock(mutex);

        // Only own entities can be removed; an entity stored only in an inner layer is read-only here
        if (!own_entities.contains(key))
        {
            if (inherited_entities.contains(key))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "It is not allowed to remove workload entity '{}' that is stored in read-only {} storage", entity_name, next_storage->getName());
            if (throw_if_not_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' doesn't exist", entity_type == WorkloadEntityType::Resource ? "Resource" : "Workload", entity_name);
            return false;
        }

        // Entities of inner layers cannot reference own entities, so only own entities are checked
        String names;
        for (const auto & [source_key, source_entity] : own_entities)
        {
            forEachReference(source_entity, [&](const WorkloadEntityKey & target, const String & source)
            {
                if (target == key)
                    names += " " + source;
            });
        }
        if (!names.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' cannot be dropped. It is referenced by:{}", entity_name, names);

        auto result = removeEntityImpl(current_context, entity_type, entity_name, throw_if_not_exists);
        if (result == OperationResult::Ok)
            own_entities.erase(key);

        if (result == OperationResult::Retry)
            continue; // Entities were updated, we need to rerun all the validations

        if (result == OperationResult::Ok)
            updateVisibleEntities(lock);

        return result == OperationResult::Ok;
    }
}

scope_guard WorkloadEntityStorageBase::getAllEntitiesAndSubscribe(const OnChangedHandler & handler)
{
    scope_guard result;

    std::vector<Event> current_state;
    std::unique_lock notify_lock{notify_mutex, std::defer_lock};
    {
        std::lock_guard lock{mutex};
        current_state = orderEntities(visible_entities);
        // Taken before releasing `mutex`, so the snapshot is delivered before any later transaction
        notify_lock.lock();

        std::lock_guard lock2{handlers->mutex};
        handlers->list.push_back(std::make_shared<HandlerEntry>(handler));
        auto handler_it = std::prev(handlers->list.end());
        result = [my_handlers = handlers, entry = *handler_it, handler_it]
        {
            {
                std::lock_guard lock3{my_handlers->mutex};
                my_handlers->list.erase(handler_it);
            }

            std::lock_guard exec_lock{entry->exec_mutex};
            entry->unsubscribed = true;
        };
    }

    // When you subscribe you get all the entities back to your handler immediately if already loaded, or later when loaded
    handler(current_state);

    return result;
}

String WorkloadEntityStorageBase::getMasterThreadResourceName()
{
    std::lock_guard lock{mutex};
    return master_thread_resource;
}

String WorkloadEntityStorageBase::getWorkerThreadResourceName()
{
    std::lock_guard lock{mutex};
    return worker_thread_resource;
}

String WorkloadEntityStorageBase::getQueryResourceName()
{
    std::lock_guard lock{mutex};
    return query_resource;
}

String WorkloadEntityStorageBase::getMemoryReservationResourceName()
{
    std::lock_guard lock{mutex};
    return memory_reservation_resource;
}

void WorkloadEntityStorageBase::updateRoleResources()
{
    master_thread_resource.clear();
    worker_thread_resource.clear();
    query_resource.clear();
    memory_reservation_resource.clear();

    // Visible entities never conflict, so every role is served by at most one visible resource
    for (const auto & [key, entity] : visible_entities)
    {
        auto * resource = typeid_cast<ASTCreateResourceQuery *>(entity.get());
        if (!resource)
            continue;
        for (const auto & operation : resource->operations)
        {
            switch (operation.mode)
            {
                case ResourceAccessMode::MasterThread: master_thread_resource = key.name; break;
                case ResourceAccessMode::WorkerThread: worker_thread_resource = key.name; break;
                case ResourceAccessMode::Query: query_resource = key.name; break;
                case ResourceAccessMode::MemoryReservation: memory_reservation_resource = key.name; break;
                case ResourceAccessMode::DiskRead:
                case ResourceAccessMode::DiskWrite:
                    break;
            }
        }
    }
}

ASTPtr WorkloadEntityStorageBase::findKnownEntity(const WorkloadEntityKey & key, const WorkloadEntities & own) const
{
    if (auto it = inherited_entities.find(key); it != inherited_entities.end())
        return it->second;
    if (auto it = own.find(key); it != own.end())
        return it->second;
    return nullptr;
}

void WorkloadEntityStorageBase::checkClaimConflicts(const WorkloadEntityKey & key, const ASTPtr & entity) const
{
    for (const auto & claim : getClaimKeys(key, entity))
    {
        if (!std::holds_alternative<ResourceAccessMode>(claim))
            continue; // An entity with the same key is either replaced or reported by the caller
        for (const auto * layer_entities : {&own_entities, &inherited_entities})
        {
            for (const auto & [other_key, other_entity] : *layer_entities)
            {
                auto other_claims = getClaimKeys(other_key, other_entity);
                if (other_key != key && std::find(other_claims.begin(), other_claims.end(), claim) != other_claims.end())
                    throwClaimConflict(claim, other_key.name);
            }
        }
    }
}

void WorkloadEntityStorageBase::checkLoadedReferences(const WorkloadEntities & new_own_entities) const
{
    for (const auto & [key, entity] : new_own_entities)
    {
        forEachReference(entity, [&](const WorkloadEntityKey & target, const String & source)
        {
            if (!findKnownEntity(target, new_own_entities))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload '{}' references {} '{}' that doesn't exist", source, getKindName(target.type), target.name);
        });
    }

    // Inherited entities cannot reference own entities, so a cycle consists of own entities only
    checkParentCycles(new_own_entities);
}

WorkloadEntities WorkloadEntityStorageBase::computeVisibleEntities(const WorkloadEntities & own) const
{
    std::set<ClaimKey> inherited_claims;
    for (const auto & [key, entity] : inherited_entities)
    {
        for (auto & claim : getClaimKeys(key, entity))
            inherited_claims.insert(std::move(claim));
    }

    // An own entity is shadowed if an inherited entity claims any of its keys
    WorkloadEntities result = inherited_entities;
    for (const auto & [key, entity] : own)
    {
        auto claims = getClaimKeys(key, entity);
        if (std::none_of(claims.begin(), claims.end(), [&](const ClaimKey & claim) { return inherited_claims.contains(claim); }))
            result.emplace(key, entity);
    }

    return result;
}

void WorkloadEntityStorageBase::updateVisibleEntities(std::unique_lock<std::recursive_mutex> & lock)
{
    WorkloadEntities new_visible_entities = computeVisibleEntities(own_entities);

    // Fill vector of `changes` based on difference between current and new visible entities
    std::vector<EntityChange> changes;
    for (const auto & [key, entity] : visible_entities)
    {
        if (auto it = new_visible_entities.find(key); it != new_visible_entities.end())
        {
            if (!entityEquals(entity, it->second))
                changes.emplace_back(key, entity, it->second);
        }
        else
            changes.emplace_back(key, entity, ASTPtr{});
    }
    for (const auto & [key, entity] : new_visible_entities)
    {
        if (!visible_entities.contains(key))
            changes.emplace_back(key, ASTPtr{}, entity);
    }

    // Sort `changes` to respect consistency of references
    std::vector<Event> tx;
    for (const auto & change : topologicallySortedChanges(changes))
    {
        for (auto & event : change.toEvents())
        {
            if (event.entity)
                LOG_DEBUG(log, "Create or replace workload entity: {}", event.entity->formatForLogging());
            else
                LOG_DEBUG(log, "Drop {}: {}", getKindName(event.type), event.name);
            tx.push_back(std::move(event));
        }
    }
    visible_entities = std::move(new_visible_entities);

    updateRoleResources();

    unlockAndNotify(lock, tx);
}

void WorkloadEntityStorageBase::unlockAndNotify(
    std::unique_lock<std::recursive_mutex> & lock,
    const std::vector<Event> & tx)
{
    if (tx.empty())
        return;

    std::vector<HandlerEntryPtr> current_handlers;
    {
        std::lock_guard handlers_lock{handlers->mutex};
        boost::range::copy(handlers->list, std::back_inserter(current_handlers));
    }

    // Taken before releasing `lock`, so transactions are delivered one at a time in the order they were computed
    std::lock_guard notify_lock{notify_mutex};
    lock.unlock();

    for (const auto & entry : current_handlers)
    {
        try
        {
            std::lock_guard exec_lock{entry->exec_mutex};
            if (entry->unsubscribed)
                continue;
            entry->handler(tx);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

std::unique_lock<std::recursive_mutex> WorkloadEntityStorageBase::getLock() const
{
    return std::unique_lock{mutex};
}

void WorkloadEntityStorageBase::setOwnEntities(const ASTs & queries)
{
    WorkloadEntities new_own_entities;
    std::map<ClaimKey, String> claims;
    for (const auto & query : queries)
    {
        ASTPtr entity = normalizeCreateWorkloadEntityQuery(*query);
        WorkloadEntityKey key = getEntityKey(entity);
        checkUserEntityName(key.name);
        if (!new_own_entities.emplace(key, entity).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The {} '{}' is defined more than once", getKindName(key.type), key.name);
        for (auto & claim : getClaimKeys(key, entity))
        {
            if (auto [it, inserted] = claims.emplace(claim, key.name); !inserted)
                throwClaimConflict(claim, it->second);
        }
    }

    std::unique_lock lock(mutex);

    for (const auto & [key, entity] : new_own_entities)
    {
        auto old_it = own_entities.find(key);
        if (old_it != own_entities.end() && resourceUnitChanged(old_it->second, entity))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cost unit of resource '{}' cannot be changed; drop and recreate the resource instead.",
                key.name);

        // Validate setting values of new and changed workloads. Unknown setting names are allowed, like in
        // `WorkloadResourceManager`, so that entities written by a newer version can be loaded.
        auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(entity.get());
        if (!workload || (old_it != own_entities.end() && entityEquals(old_it->second, entity)))
            continue;
        WorkloadSettings validator;
        validator.initFromChanges(workload->changes, /*resource_name=*/{}, /*throw_on_unknown_setting=*/false);
        forEachReference(entity, [&](const WorkloadEntityKey & target, const String &)
        {
            if (target.type == WorkloadEntityType::Resource)
            {
                WorkloadSettings resource_validator;
                resource_validator.initFromChanges(workload->changes, target.name, /*throw_on_unknown_setting=*/false);
            }
        });
    }

    checkLoadedReferences(new_own_entities);

    own_entities = std::move(new_own_entities);
    updateVisibleEntities(lock);
}

void WorkloadEntityStorageBase::replaceOwnEntities(std::unique_lock<std::recursive_mutex> & lock, WorkloadEntities new_own_entities)
{
    own_entities = std::move(new_own_entities);
    updateVisibleEntities(lock);
}

WorkloadEntities WorkloadEntityStorageBase::getAllEntities() const
{
    std::lock_guard lock{mutex};
    return visible_entities;
}

bool WorkloadEntityStorageBase::isIndirectlyReferenced(const WorkloadEntityKey & target, const WorkloadEntityKey & source) const
{
    // Shadowed own entities are walked too: they may become visible again, together with their references
    std::queue<WorkloadEntityKey> bfs;
    std::set<WorkloadEntityKey> visited;
    visited.insert(source);
    bfs.push(source);
    while (!bfs.empty())
    {
        WorkloadEntityKey current = bfs.front();
        bfs.pop();
        if (current == target)
            return true;
        for (const auto * layer_entities : {&own_entities, &inherited_entities})
        {
            auto it = layer_entities->find(current);
            if (it == layer_entities->end())
                continue;
            forEachReference(it->second, [&](const WorkloadEntityKey & next, const String &)
            {
                if (visited.insert(next).second)
                    bfs.push(next);
            });
        }
    }
    return false;
}

std::vector<WorkloadEntityStorageBase::Event> WorkloadEntityStorageBase::orderEntities(
    const WorkloadEntities & entities,
    const std::optional<Event> & change)
{
    WorkloadEntities all_entities = entities;
    if (change && change->entity)
        all_entities[{change->type, change->name}] = change->entity;
    else if (change)
        all_entities.erase({change->type, change->name});

    // Resources should go first because workloads could reference them
    std::vector<Event> result;
    std::unordered_map<String, ASTPtr> workloads;
    for (const auto & [key, ast] : all_entities)
    {
        if (key.type == WorkloadEntityType::Resource)
            result.emplace_back(key.type, key.name, ast);
        else
            workloads.emplace(key.name, ast);
    }

    // Workloads should go in an order such that children are enlisted only after its parent
    for (auto & [entity_name, ast] : topologicallySortedWorkloads(workloads))
        result.emplace_back(WorkloadEntityType::Workload, entity_name, ast);

    return result;
}

String WorkloadEntityStorageBase::serializeOwnEntities(std::optional<Event> change)
{
    auto ordered_entities = orderEntities(own_entities, change);
    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(/*one_line=*/true);
    for (const auto & event : ordered_entities)
    {
        event.entity->format(buf, settings);
        buf.write(";\n", 2);
    }
    return buf.str();
}

ASTs WorkloadEntityStorageBase::parseEntitiesFromString(const String & data, LoggerPtr log)
{
    ASTs queries;
    ParserCreateWorkloadEntity parser;
    const char * begin = data.data(); /// begin of current query
    const char * pos = begin; /// parser moves pos from begin to the end of current query
    const char * end = begin + data.size();
    while (pos < end)
    {
        queries.emplace_back(parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
        LOG_TRACE(log, "Parsed entity definition: {}", queries.back()->formatForLogging());
        getEntityKey(queries.back()); // Throws if it is neither a workload nor a resource
        while (isWhitespaceASCII(*pos) || *pos == ';')
            ++pos;
    }
    return queries;
}

void WorkloadEntityStorageBase::backup(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    WorkloadEntityType entity_type) const
{
    /// system.workloads and system.resources are backed up via two separate backup() calls on this shared storage.
    /// Take a single consistent snapshot of all own entities for the whole backup operation (keyed by the backup
    /// UUID) and serve both calls from it, so that a DDL committing between the two calls cannot split an
    /// interdependent pair across the snapshot. Otherwise "CREATE RESOURCE r; CREATE WORKLOAD w SETTINGS ... FOR r"
    /// committing between a resources snapshot and a workloads snapshot could put w.sql in the backup without r.sql,
    /// producing a backup that fails to restore on the "FOR r" reference. For replicated storage the coordination
    /// elects one host to write both entity types, so a per-host consistent snapshot is sufficient.
    const UUID backup_id = backup_entries_collector.getBackupSettings().backup_uuid.value();

    std::vector<std::pair<String, BackupEntryPtr>> backup_entries;
    {
        std::lock_guard lock{mutex};

        /// Back up only entities defined via SQL and stored in this storage (`own_entities`).
        /// Entities provided through the server configuration are kept in the next storage in the
        /// chain (WorkloadEntityConfigStorage) and must NOT be backed up: the configuration is
        /// assumed to be backed up and restored separately together with all its entities.
        /// Re-creating a config entity from a backup would persist it as a SQL entity, which is not
        /// equivalent. Note `getAllEntities()` returns the merged view (including config entities),
        /// so it must not be used here.
        auto [it, inserted] = entities_to_backup.try_emplace(backup_id);
        if (inserted)
        {
            it->second = own_entities;
            /// Drop the snapshot when the backup operation ends, whether it succeeds or fails. The cleanup runs
            /// from the scope_guard's destructor, held alive by a post task, so it fires even if a later table's
            /// backupData() throws before post tasks run (post tasks run only after all backupData() calls
            /// succeed). Otherwise the snapshot would stay pinned on this shared storage, and a retry reusing the
            /// same backup_uuid would serve this stale snapshot instead of the current own_entities.
            auto cleanup = std::make_shared<scope_guard>([this, backup_id]
            {
                std::lock_guard cleanup_lock{mutex};
                entities_to_backup.erase(backup_id);
            });
            backup_entries_collector.addPostTask([cleanup]{});
        }

        for (const auto & [key, ast] : it->second)
        {
            if (key.type != entity_type)
                continue;
            /// Warn when an entity being backed up references one defined in the server configuration: such
            /// config-defined entities are not part of the backup (they belong to the server config), so a
            /// restore on a server that lacks them in its config will fail. Evaluated only over the entities
            /// selected for this call (past the entity_type filter above), so a resources-only backup does not
            /// warn about workloads and vice versa. `inherited_entities` holds the config-defined entities.
            forEachReference(ast, [&](const WorkloadEntityKey & target, const String & source)
            {
                if (!it->second.contains(target) && inherited_entities.contains(target))
                    LOG_WARNING(log, "Backed up workload entity '{}' references {} '{}' defined in the server "
                        "configuration, which is not included in the backup; restoring it requires '{}' to "
                        "already exist on the destination server.",
                        source, target.type == WorkloadEntityType::Resource ? "resource" : "parent workload", target.name, target.name);
            });
            backup_entries.emplace_back(
                escapeForFileName(key.name) + ".sql",
                std::make_shared<BackupEntryFromMemory>(ast->formatWithSecretsOneLine()));
        }
    }

    if (!isReplicated())
    {
        fs::path data_path_in_backup_fs{data_path_in_backup};
        for (const auto & [file_name, entry] : backup_entries)
            backup_entries_collector.addBackupEntry(data_path_in_backup_fs / file_name, entry);
        return;
    }

    String replication_id = getReplicationID();
    auto backup_coordination = backup_entries_collector.getBackupCoordination();
    backup_coordination->addReplicatedWorkloadEntitiesDir(replication_id, entity_type, data_path_in_backup);

    /// On the stage of running post tasks, all directories will already be added to the backup coordination object.
    /// They will only be returned for one of the hosts below, for the rest an empty list.
    /// See also BackupCoordinationReplicatedWorkloadEntities class.
    backup_entries_collector.addPostTask(
        [my_backup_entries = std::move(backup_entries),
         my_replication_id = std::move(replication_id),
         entity_type,
         &backup_entries_collector,
         backup_coordination]
        {
            auto dirs = backup_coordination->getReplicatedWorkloadEntitiesDirs(my_replication_id, entity_type);

            for (const auto & dir : dirs)
            {
                fs::path dir_fs{dir};
                for (const auto & [file_name, entry] : my_backup_entries)
                    backup_entries_collector.addBackupEntry(dir_fs / file_name, entry);
            }
        });
}

void WorkloadEntityStorageBase::restore(
    RestorerFromBackup & restorer,
    const String & data_path_in_backup,
    WorkloadEntityType entity_type)
{
    if (isReplicated()
        && !restorer.getRestoreCoordination()->acquireReplicatedWorkloadEntities(getReplicationID()))
        return; /// Other replica is already restoring the workload entities.

    auto backup = restorer.getBackup();
    fs::path data_path_in_backup_fs{data_path_in_backup};

    Strings filenames = backup->listFiles(data_path_in_backup, /*recursive*/ false);
    if (filenames.empty())
        return; /// Nothing to restore.

    for (const auto & filename : filenames)
    {
        if (!filename.ends_with(".sql"))
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot restore workload entities: File name {} doesn't have the extension .sql",
                String{data_path_in_backup_fs / filename});
    }

    ASTs parsed_entities;
    for (const auto & filename : filenames)
    {
        String filepath = data_path_in_backup_fs / filename;
        auto in = backup->readFile(filepath);
        String statement_def;
        readStringUntilEOF(statement_def, *in);

        for (auto & ast : parseEntitiesFromString(statement_def, log))
        {
            /// Reject a definition whose kind does not match the system table being restored. The two system
            /// tables carry independent access checks (system.workloads requires CREATE_WORKLOAD, system.resources
            /// requires CREATE_RESOURCE; see RestorerFromBackup::checkAccessForObjectsFoundInBackup), so accepting
            /// a WORKLOAD from the resources directory (or vice versa) would bypass the access gate of the other
            /// kind and violate the system-table backup contract.
            if (getEntityKey(ast).type != entity_type)
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore workload entities: file {} defines an entity of a kind that does not match the "
                    "system table being restored",
                    String{data_path_in_backup_fs / filename});
            parsed_entities.push_back(std::move(ast));
        }
    }

    /// WORKLOADs and RESOURCEs are backed up (and restored) via two separate system tables (system.workloads and
    /// system.resources), so their restore tasks are scheduled independently. We accumulate the parsed entities of
    /// both types and create them all together, in an order that keeps every reference valid (resources first, then
    /// workloads parent-first). Concurrent restores are allowed (backups.allow_concurrent_restores), so the
    /// accumulator is keyed by the unique restore UUID instead of being kept as shared storage state; otherwise two
    /// concurrent restores would mix their entities.
    const auto & restore_settings = restorer.getRestoreSettings();
    const UUID restore_id = restore_settings.restore_uuid.value();
    /// WORKLOAD and RESOURCE entities share a single creation mode (create_workloads_and_resources): they are
    /// intertwined and always restored together, so handling pre-existing entities differently per type makes no
    /// sense. kCreateIfNotExists (default) skips existing entities, kCreate fails the RESTORE, kReplace overwrites.
    const bool throw_if_exists = (restore_settings.create_workloads_and_resources == RestoreWorkloadsAndResourcesCreationMode::kCreate);
    const bool replace_if_exists = (restore_settings.create_workloads_and_resources == RestoreWorkloadsAndResourcesCreationMode::kReplace);

    {
        std::lock_guard lock{mutex};
        auto & accumulated = entities_to_restore[restore_id];
        for (auto & ast : parsed_entities)
            accumulated.emplace(getEntityKey(ast), ast);
    }

    /// Register the deferred restore task on every restore() call (i.e. once for each of the two system tables).
    /// The task is idempotent: the first one to run swaps out and creates all entities accumulated for this restore
    /// operation, and any later task finds the accumulator already drained and does nothing. Data restore tasks run
    /// only after restore() has been called for both tables of the operation, so every referenced entity is present
    /// by the time entities are created.
    ///
    /// The accumulator must not outlive the restore operation. A scope_guard held by the restore task drops it when
    /// the operation ends, on success or failure, so a restore that aborts before the data restore tasks run (e.g.
    /// another table throws) does not pin these ASTs on this shared storage; a retry reusing the same restore_uuid
    /// then starts from an empty accumulator instead of mixing in the stale entities.
    auto restore_context = restorer.getContext();
    auto cleanup = std::make_shared<scope_guard>([this, restore_id]
    {
        std::lock_guard lock{mutex};
        entities_to_restore.erase(restore_id);
    });
    restorer.addDataRestoreTask(
        [this, restore_context, restore_id, throw_if_exists, replace_if_exists, cleanup]
        { restoreEntitiesAccumulatedFromBackup(restore_context, restore_id, throw_if_exists, replace_if_exists); });
}

void WorkloadEntityStorageBase::restoreEntitiesAccumulatedFromBackup(
    const ContextMutablePtr & context, const UUID & restore_id, bool throw_if_exists, bool replace_if_exists)
{
    WorkloadEntities to_restore;
    {
        std::lock_guard lock{mutex};
        if (auto node = entities_to_restore.extract(restore_id))
            to_restore = std::move(node.mapped());
    }

    if (to_restore.empty())
        return;

    /// Every entity a restored workload references (its parent workload, and the RESOURCEs used in a
    /// `SETTINGS ... FOR ...` clause) must exist once the restore completes: either it is part of this backup
    /// or it is already present on the server. Otherwise creating the workload fails with an opaque
    /// "references ... that doesn't exist" error. The usual cause is backing up / restoring only
    /// `system.workloads` while a workload depends on a SQL-defined RESOURCE -- resources are not pulled into a
    /// workloads-only backup, so the two system tables must be backed up and restored together. Fail early with
    /// a clear, actionable message before creating anything.
    ///
    /// Only for non-replicated storage, where the in-memory maps are the authoritative single-node state. On
    /// replicated (Keeper-backed) storage the local caches can lag Keeper until a watch fires, so this
    /// pre-check could wrongly reject a dependency that is actually committed; there we leave `storeEntity` to
    /// enforce references as before. Validating the replicated path against authoritative Keeper state is a
    /// separate change.
    if (!isReplicated())
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : to_restore)
        {
            forEachReference(entry.second, [&](const WorkloadEntityKey & target, const String & source)
            {
                if (to_restore.contains(target) || visible_entities.contains(target))
                    return;
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore workload entities: workload '{}' references {} '{}' which is neither included "
                    "in the backup nor present on the server. WORKLOADs and RESOURCEs must be backed up and "
                    "restored together (include both TABLE system.workloads and TABLE system.resources in the "
                    "same BACKUP/RESTORE command).",
                    source,
                    target.type == WorkloadEntityType::Resource ? "resource" : "parent workload",
                    target.name);
            });
        }
    }

    /// orderEntities() returns resources first and then workloads in parent-first order, so that every reference
    /// (workload parent, FOR resource) is already created by the time it is needed.
    for (const auto & event : orderEntities(to_restore))
    {
        /// throw_if_exists / replace_if_exists come from the create_workloads_and_resources restore setting and are
        /// applied uniformly to every workload and resource (see restore()).
        storeEntity(
            context,
            event.type,
            event.name,
            event.entity,
            throw_if_exists,
            replace_if_exists,
            context->getSettingsRef());
    }
}

}
