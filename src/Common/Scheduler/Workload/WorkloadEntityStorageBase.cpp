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

#include <boost/container/flat_set.hpp>
#include <boost/range/algorithm/copy.hpp>

#include <map>
#include <mutex>
#include <queue>
#include <set>
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

/// Returns a type of a workload entity `ptr`
WorkloadEntityType getEntityType(const ASTPtr & ptr)
{
    if (auto * res = typeid_cast<ASTCreateWorkloadQuery *>(ptr.get()); res)
        return WorkloadEntityType::Workload;
    if (auto * res = typeid_cast<ASTCreateResourceQuery *>(ptr.get()); res)
        return WorkloadEntityType::Resource;
    chassert(false);
    return WorkloadEntityType::MAX;
}

/// Returns `workload` or `resource` for messages
std::string_view getEntityKindName(const ASTPtr & ptr)
{
    return getEntityType(ptr) == WorkloadEntityType::Workload ? "workload" : "resource";
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
bool isReservedEntityName(const String & entity_name)
{
    return entity_name == IMPLICIT_CPU_RESOURCE_NAME || entity_name == IMPLICIT_MEMORY_RESOURCE_NAME;
}

void checkUserEntityName(const String & entity_name)
{
    if (entity_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity name should not be empty.");
    if (isReservedEntityName(entity_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Workload entity name '{}' is reserved for an implicit resource and cannot be used", entity_name);
}

/// A claim key (see IWorkloadEntityStorage.h): an entity name or an access-mode role of a resource
using ClaimKey = std::variant<String, ResourceAccessMode>;

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

String claimKeyToString(const ClaimKey & key)
{
    if (const auto * name = std::get_if<String>(&key))
        return fmt::format("name '{}'", *name);
    return roleToString(std::get<ResourceAccessMode>(key));
}

/// Returns all claim keys of an entity
std::vector<ClaimKey> getClaimKeys(const String & entity_name, const ASTPtr & entity)
{
    std::vector<ClaimKey> keys{ClaimKey{entity_name}};
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
                    ClaimKey key{operation.mode};
                    if (std::find(keys.begin(), keys.end(), key) == keys.end())
                        keys.push_back(std::move(key));
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

bool claims(const String & entity_name, const ASTPtr & entity, const ClaimKey & key)
{
    auto keys = getClaimKeys(entity_name, entity);
    return std::find(keys.begin(), keys.end(), key) != keys.end();
}

[[noreturn]] void throwClaimConflict(const ClaimKey & key, const String & holder)
{
    if (const auto * mode = std::get_if<ResourceAccessMode>(&key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second resource for {} is not allowed. Current resource name: '{}'.", roleToString(*mode), holder);
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists", holder);
}

/// Workload entities could reference each other.
/// This enum defines all possible reference types
enum class ReferenceType
{
    Parent, // Source workload references target workload as a parent
    ForResource // Source workload references target resource in its `SETTINGS x = y FOR resource` clause
};

/// Runs a `func` callback for every reference from `source` to `target`.
/// This function is the source of truth defining what `target` references are stored in a workload `source_entity`
void forEachReference(
    const ASTPtr & source_entity,
    std::function<void(const String & target, const String & source, ReferenceType type)> func)
{
    if (auto * res = typeid_cast<ASTCreateWorkloadQuery *>(source_entity.get()))
    {
        // Parent reference
        String parent = res->getWorkloadParent();
        if (!parent.empty())
            func(parent, res->getWorkloadName(), ReferenceType::Parent);

        // References to RESOURCEs mentioned in SETTINGS clause after FOR keyword
        std::unordered_set<String> resources;
        for (const auto & [name, value, resource] : res->changes)
        {
            if (!resource.empty())
                resources.insert(resource);
        }
        for (const String & resource : resources)
            func(resource, res->getWorkloadName(), ReferenceType::ForResource);
    }
    if (auto * res = typeid_cast<ASTCreateResourceQuery *>(source_entity.get()); res)
    {
        // RESOURCE has no references to be validated, we allow mentioned disks to be created later
    }
}

/// Returns the names of workloads of `entities` that lie on a parent cycle. Every workload has at most one parent,
/// so a walk up from any workload either leaves `entities`, reaches a parentless one or enters a cycle.
std::unordered_set<String> findParentCycles(const std::unordered_map<String, ASTPtr> & entities)
{
    std::unordered_set<String> on_cycle;
    std::unordered_set<String> walked;
    for (const auto & [entity_name, entity] : entities)
    {
        std::unordered_map<String, size_t> position; /// Position of a name in the current walk
        std::vector<String> path;
        String current = entity_name;
        while (!walked.contains(current))
        {
            if (auto pos = position.find(current); pos != position.end())
            {
                on_cycle.insert(path.begin() + pos->second, path.end());
                break;
            }
            auto it = entities.find(current);
            if (it == entities.end())
                break;
            auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(it->second.get());
            if (!workload || !workload->hasParent())
                break;
            position.emplace(current, path.size());
            path.push_back(current);
            current = workload->getWorkloadParent();
        }
        walked.insert(path.begin(), path.end());
    }
    return on_cycle;
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

/// Helper for recursive DFS
void topologicallySortedDependenciesImpl(
    const String & name,
    const std::unordered_map<String, std::unordered_set<String>> & dependencies,
    std::unordered_set<String> & visited,
    std::vector<String> & result)
{
    if (visited.contains(name))
        return;
    visited.insert(name);

    if (auto it = dependencies.find(name); it != dependencies.end())
    {
        for (const String & dep : it->second)
            topologicallySortedDependenciesImpl(dep, dependencies, visited, result);
    }

    result.emplace_back(name);
}

/// Returns nodes in topological order that respect `dependencies` (key is node name, value is set of dependencies)
std::vector<String> topologicallySortedDependencies(const std::unordered_map<String, std::unordered_set<String>> & dependencies)
{
    std::unordered_set<String> visited; // Set to track visited nodes
    std::vector<String> result; // Result to store nodes in topologically sorted order

    // Perform DFS for each node in the graph
    for (const auto & [name, _] : dependencies)
        topologicallySortedDependenciesImpl(name, dependencies, visited, result);

    return result;
}

/// Represents a change of a workload entity (WORKLOAD or RESOURCE)
struct EntityChange
{
    String name; /// Name of entity
    ASTPtr before; /// Entity before change (CREATE if not set)
    ASTPtr after; /// Entity after change (DROP if not set)

    /// True if the change drops the entity and creates a new one (type or cost unit changes)
    bool recreates() const
    {
        return before && after && (getEntityType(before) != getEntityType(after) || resourceUnitChanged(before, after));
    }

    std::vector<IWorkloadEntityStorage::Event> toEvents() const
    {
        if (!after)
            return {{getEntityType(before), name, {}}};
        else if (!before)
            return {{getEntityType(after), name, after}};
        else
        {
            auto type_before = getEntityType(before);
            auto type_after = getEntityType(after);
            // If type or cost unit changed (e.g. an inherited entity of another kind hides an own one by a conflict),
            // we have to remove an old entity and add a new one
            if (recreates())
                return {{type_before, name, {}}, {type_after, name, after}};
            else
                return {{type_after, name, after}};
        }
    }
};

/// Returns `changes` ordered for execution.
/// Every intemediate state during execution will be consistent (i.e. all references will be valid)
/// NOTE: It does not validate changes, any problem will be detected during execution.
/// NOTE: There will be no error if valid order does not exist.
std::vector<EntityChange> topologicallySortedChanges(const std::vector<EntityChange> & changes)
{
    // Construct map from entity name into entity change
    std::unordered_map<String, const EntityChange *> change_by_name;
    for (const auto & change : changes)
        change_by_name[change.name] = &change;

    // Construct references maps (before changes and after changes)
    std::unordered_map<String, std::unordered_set<String>> old_sources; // Key is target. Value is set of names of source entities.
    std::unordered_map<String, std::unordered_set<String>> new_targets; // Key is source. Value is set of names of target entities.
    for (const auto & change : changes)
    {
        if (change.before)
        {
            forEachReference(change.before,
                [&] (const String & target, const String & source, ReferenceType)
                {
                    old_sources[target].insert(source);
                });
        }
        if (change.after)
        {
            forEachReference(change.after,
                [&] (const String & target, const String & source, ReferenceType)
                {
                    new_targets[source].insert(target);
                });
        }
    }

    // There are consistency rules that regulate order in which changes must be applied (see below).
    // Construct DAG of dependencies between changes.
    std::unordered_map<String, std::unordered_set<String>> dependencies; // Key is entity name. Value is set of names of entity that should be changed first.
    for (const auto & change : changes)
    {
        dependencies.emplace(change.name, std::unordered_set<String>{}); // Make sure we create nodes that have no dependencies
        for (const auto & event : change.toEvents())
        {
            if (!event.entity) // DROP
            {
                // Rule 1: Entity can only be removed after all existing references to it are removed as well.
                for (const String & source : old_sources[event.name])
                {
                    if (change_by_name.contains(source))
                        dependencies[event.name].insert(source);
                }
            }
            else // CREATE || CREATE OR REPLACE
            {
                // Rule 2: Entity can only be created after all entities it references are created as well.
                for (const String & target : new_targets[event.name])
                {
                    if (auto it = change_by_name.find(target); it != change_by_name.end())
                    {
                        const EntityChange & target_change = *it->second;
                        // If target is creating (or recreating), it should be created first.
                        // (But if target is updating, there is no dependency).
                        if (!target_change.before || target_change.recreates())
                            dependencies[event.name].insert(target);
                    }
                }
            }
        }
    }

    // Topological sort of changes to respect consistency rules
    std::vector<EntityChange> result;
    for (const String & name : topologicallySortedDependencies(dependencies))
        result.push_back(*change_by_name[name]);

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
                        inherited_entities[event.name] = event.entity;
                    else
                        inherited_entities.erase(event.name);
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

ASTPtr WorkloadEntityStorageBase::get(const String & entity_name) const
{
    if (auto result = tryGet(entity_name))
        return result;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "The workload entity name '{}' is not saved",
        entity_name);
}

ASTPtr WorkloadEntityStorageBase::tryGet(const String & entity_name) const
{
    std::lock_guard lock(mutex);

    auto it = visible_entities.find(entity_name);
    if (it == visible_entities.end())
        return nullptr;

    return it->second;
}

bool WorkloadEntityStorageBase::has(const String & entity_name) const
{
    return tryGet(entity_name) != nullptr;
}

String WorkloadEntityStorageBase::getEntitySource(const String & entity_name) const
{
    {
        std::lock_guard lock(mutex);
        if (!visible_entities.contains(entity_name))
            return "";
        if (!inherited_entities.contains(entity_name))
            return String(getName());
    }
    // Not under the lock: the next storage may notify this one while holding its own lock
    return next_storage->getEntitySource(entity_name);
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
    auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(create_entity_query.get());
    auto * resource = typeid_cast<ASTCreateResourceQuery *>(create_entity_query.get());

    while (true)
    {
        std::unique_lock lock{mutex};

        // An entity with this name may be stored in this layer (possibly shadowed) or in an inner one
        ASTPtr old_entity = findKnownEntity(entity_name, own_entities); // entity to be REPLACED
        if (old_entity && !throw_if_exists && !replace_if_exists)
            return false;

        // An inherited entity of another kind with this name is a conflict
        if (auto it = inherited_entities.find(entity_name); it != inherited_entities.end() && getEntityType(it->second) != getEntityType(create_entity_query))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' conflicts with the {} of the same name in {} storage",
                entity_name, getEntityKindName(it->second), next_storage->getName());

        if (old_entity && throw_if_exists)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists", entity_name);

        // Validate CREATE OR REPLACE
        if (old_entity)
        {
            auto * old_workload = typeid_cast<ASTCreateWorkloadQuery *>(old_entity.get());
            auto * old_resource = typeid_cast<ASTCreateResourceQuery *>(old_entity.get());
            if (workload && !old_workload)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists, but it is not a workload", entity_name);
            if (resource && !old_resource)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists, but it is not a resource", entity_name);
            if (inherited_entities.contains(entity_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "It is not allowed to replace workload entity '{}' that is stored in read-only {} storage", entity_name, next_storage->getName());
        }

        // Validate workload
        if (workload)
        {
            // Check the settings values and throw if something is wrong
            WorkloadSettings validator;
            validator.initFromChanges(workload->changes);
        }

        // Validate resource: cost unit cannot change via CREATE OR REPLACE — the scheduler
        // hierarchy is built with unit-specific node types and would silently end up with the
        // wrong scheduler/link type in release builds.
        if (resource && old_entity)
        {
            auto * old_resource = typeid_cast<ASTCreateResourceQuery *>(old_entity.get());
            if (old_resource && old_resource->unit != resource->unit)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cost unit of resource '{}' cannot be changed via CREATE OR REPLACE; drop and recreate the resource instead.",
                    entity_name);
        }

        // Conflicts with own and inherited entities: a role must be served by at most one resource
        checkClaimConflicts(entity_name, create_entity_query);
        checkInboundReferences(entity_name, create_entity_query);

        // References are resolved within own and inherited entities, inherited first, so entities of outer layers
        // cannot be referenced
        forEachReference(create_entity_query,
            [this, workload] (const String & target, const String & source, ReferenceType type)
            {
                ASTPtr target_entity = findKnownEntity(target, own_entities);
                if (!target_entity)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' references another workload entity '{}' that doesn't exist", source, target);

                switch (type)
                {
                    case ReferenceType::Parent:
                    {
                        if (typeid_cast<ASTCreateWorkloadQuery *>(target_entity.get()) == nullptr)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload parent should reference another workload, not '{}'.", target);
                        break;
                    }
                    case ReferenceType::ForResource:
                    {
                        auto * target_resource = typeid_cast<ASTCreateResourceQuery *>(target_entity.get());
                        if (target_resource == nullptr)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload settings should reference resource in FOR clause, not '{}'.", target);

                        // Validate that we could parse the settings for specific resource
                        WorkloadSettings validator;
                        validator.initFromChanges(workload->changes, target);
                        break;
                    }
                }

                // Detect reference cycles.
                // The only way to create a cycle is to add an edge that will be a part of a new cycle.
                // We are going to add an edge: `source` -> `target`, so we ensure there is no path back `target` -> `source`.
                if (isIndirectlyReferenced(source, target))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity cycles are not allowed");
            });

        auto result = storeEntityImpl(
            current_context,
            entity_type,
            entity_name,
            create_entity_query,
            throw_if_exists,
            replace_if_exists,
            settings);

        if (result == OperationResult::Retry)
            continue; // Entities were updated, we need to rerun all the validations

        if (result == OperationResult::Ok)
        {
            own_entities[entity_name] = create_entity_query;
            updateVisibleEntities(lock);
        }

        return result == OperationResult::Ok;
    }
}

bool WorkloadEntityStorageBase::removeEntity(
    const ContextPtr & current_context,
    WorkloadEntityType entity_type,
    const String & entity_name,
    bool throw_if_not_exists)
{
    while (true)
    {
        std::unique_lock lock(mutex);
        if (!findKnownEntity(entity_name, own_entities))
        {
            if (throw_if_not_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' doesn't exist", entity_name);
            else
                return false;
        }

        // Only own entities can be removed; an entity stored only in an inner layer is read-only here
        const bool inherited = inherited_entities.contains(entity_name);
        // The own entity must be of the requested kind: an own entity of the other kind may be hidden by a conflict
        // with the inherited entity of this name
        auto own_it = own_entities.find(entity_name);
        if (own_it != own_entities.end() && getEntityType(own_it->second) != entity_type)
            own_it = own_entities.end();
        if (own_it == own_entities.end())
        {
            if (auto it = inherited_entities.find(entity_name); it != inherited_entities.end() && getEntityType(it->second) == entity_type)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "It is not allowed to remove workload entity '{}' that is stored in read-only {} storage", entity_name, next_storage->getName());
            if (throw_if_not_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' doesn't exist", entity_type == WorkloadEntityType::Resource ? "Resource" : "Workload", entity_name);
            return false;
        }

        // Referenced by visible entities or by own entities (hidden ones may become visible again later).
        // An own entity hidden by an inherited one of the same name is not the target of visible references:
        // they reach the inherited one.
        std::set<String> referencing;
        if (auto reference_it = references.find(entity_name); !inherited && reference_it != references.end())
            referencing.insert(reference_it->second.begin(), reference_it->second.end());
        for (const auto & [own_name, own_entity] : own_entities)
        {
            forEachReference(own_entity, [&](const String & target, const String & source, ReferenceType)
            {
                if (target == entity_name)
                    referencing.insert(source);
            });
        }
        if (!referencing.empty())
        {
            String names;
            for (const String & name : referencing)
                names += " " + name;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' cannot be dropped. It is referenced by:{}", entity_name, names);
        }

        auto result = removeEntityImpl(
            current_context,
            entity_type,
            entity_name,
            throw_if_not_exists);

        if (result == OperationResult::Retry)
            continue; // Entities were updated, we need to rerun all the validations

        if (result == OperationResult::Ok)
        {
            own_entities.erase(entity_name);
            updateVisibleEntities(lock);
        }

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
    for (const auto & [entity_name, entity] : visible_entities)
    {
        auto * resource = typeid_cast<ASTCreateResourceQuery *>(entity.get());
        if (!resource)
            continue;
        for (const auto & operation : resource->operations)
        {
            switch (operation.mode)
            {
                case ResourceAccessMode::MasterThread: master_thread_resource = entity_name; break;
                case ResourceAccessMode::WorkerThread: worker_thread_resource = entity_name; break;
                case ResourceAccessMode::Query: query_resource = entity_name; break;
                case ResourceAccessMode::MemoryReservation: memory_reservation_resource = entity_name; break;
                case ResourceAccessMode::DiskRead:
                case ResourceAccessMode::DiskWrite:
                    break;
            }
        }
    }
}

ASTPtr WorkloadEntityStorageBase::findKnownEntity(const String & entity_name, const std::unordered_map<String, ASTPtr> & own) const
{
    if (auto it = inherited_entities.find(entity_name); it != inherited_entities.end())
        return it->second;
    if (auto it = own.find(entity_name); it != own.end())
        return it->second;
    return nullptr;
}

void WorkloadEntityStorageBase::checkClaimConflicts(const String & entity_name, const ASTPtr & entity) const
{
    for (const auto & key : getClaimKeys(entity_name, entity))
    {
        if (std::holds_alternative<String>(key))
            continue; // Name is checked by the caller: an entity with the same name is either replaced or reported
        for (const auto * layer_entities : {&own_entities, &inherited_entities})
        {
            for (const auto & [other_name, other_entity] : *layer_entities)
            {
                if (other_name != entity_name && claims(other_name, other_entity, key))
                    throwClaimConflict(key, other_name);
            }
        }
    }
}

void WorkloadEntityStorageBase::checkInboundReferences(const String & entity_name, const ASTPtr & entity) const
{
    // E.g. a hidden persisted workload may name a parent that does not exist yet
    const bool is_resource = getEntityType(entity) == WorkloadEntityType::Resource;
    for (const auto * layer_entities : {&own_entities, &inherited_entities})
    {
        for (const auto & [source_name, source_entity] : *layer_entities)
        {
            forEachReference(source_entity, [&](const String & target, const String & source, ReferenceType type)
            {
                if (target != entity_name || is_resource == (type == ReferenceType::ForResource))
                    return;
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' cannot be a {}: workload '{}' references it as a {}",
                    entity_name, getEntityKindName(entity), source, type == ReferenceType::ForResource ? "resource" : "parent workload");
            });
        }
    }
}

void WorkloadEntityStorageBase::checkLoadedReferences(const std::unordered_map<String, ASTPtr> & new_own_entities) const
{
    for (const auto & [entity_name, entity] : new_own_entities)
    {
        forEachReference(entity, [&](const String & target, const String & source, ReferenceType type)
        {
            ASTPtr target_entity = findKnownEntity(target, new_own_entities);
            if (!target_entity)
            {
                if (own_entities.contains(target))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' cannot be dropped. It is referenced by: {}", target, source);
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' references another workload entity '{}' that doesn't exist", source, target);
            }
            if (type == ReferenceType::Parent && typeid_cast<ASTCreateWorkloadQuery *>(target_entity.get()) == nullptr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload parent should reference another workload, not '{}'.", target);
            if (type == ReferenceType::ForResource && typeid_cast<ASTCreateResourceQuery *>(target_entity.get()) == nullptr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload settings should reference resource in FOR clause, not '{}'.", target);
        });
    }

    if (auto on_cycle = findParentCycles(new_own_entities); !on_cycle.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity cycles are not allowed: workload '{}' is on a parent cycle", *std::min_element(on_cycle.begin(), on_cycle.end()));
}

void WorkloadEntityStorageBase::updateVisibleEntities(std::unique_lock<std::recursive_mutex> & lock)
{
    // Conflicts between own entities exist only in persisted data loaded with `OnOwnConflict::Hide`:
    // the entity with the smaller name wins, the others are hidden
    std::map<String, ASTPtr> own_by_name(own_entities.begin(), own_entities.end());
    std::map<ClaimKey, String> own_claims;
    std::unordered_set<String> new_conflicting_entities;
    for (const auto & [entity_name, entity] : own_by_name)
    {
        if (!ownsImplicitEntities() && isReservedEntityName(entity_name))
        {
            new_conflicting_entities.insert(entity_name);
            if (!conflicting_entities.contains(entity_name))
                LOG_ERROR(log, "Workload entity '{}' of the {} storage is hidden: the name is reserved for an implicit resource. "
                    "Drop it and create it under another name.", entity_name, getName());
            continue;
        }
        auto keys = getClaimKeys(entity_name, entity);
        auto conflict = std::find_if(keys.begin(), keys.end(), [&](const ClaimKey & key) { return own_claims.contains(key); });
        if (conflict != keys.end())
        {
            new_conflicting_entities.insert(entity_name);
            if (!conflicting_entities.contains(entity_name))
                LOG_ERROR(log, "Workload entity '{}' is hidden: it conflicts with '{}' of the same {} storage, both claim {}",
                    entity_name, own_claims.at(*conflict), getName(), claimKeyToString(*conflict));
            continue;
        }
        for (auto & key : keys)
            own_claims.emplace(std::move(key), entity_name);
    }

    // Keys claimed by inherited entities. They never conflict, as they are visible entities of the inner layer.
    std::map<ClaimKey, String> inherited_claims;
    for (const auto & [entity_name, entity] : inherited_entities)
    {
        for (auto & key : getClaimKeys(entity_name, entity))
            inherited_claims.emplace(std::move(key), entity_name);
    }

    // Visible entities are inherited entities plus own entities that are neither shadowed nor conflicting.
    // An inherited entity of another kind with the same name is a conflict, not shadowing.
    std::unordered_map<String, ASTPtr> new_visible_entities = inherited_entities;
    std::unordered_set<String> new_shadowed_entities;
    for (const auto & [entity_name, entity] : own_entities)
    {
        if (new_conflicting_entities.contains(entity_name))
            continue;
        if (auto it = inherited_entities.find(entity_name); it != inherited_entities.end() && getEntityType(it->second) != getEntityType(entity))
        {
            new_conflicting_entities.insert(entity_name);
            if (!conflicting_entities.contains(entity_name))
                LOG_ERROR(log, "Workload entity '{}' of the {} storage is hidden: this {} conflicts with the {} of the same name "
                    "in {} storage. Drop one of them.", entity_name, getName(), getEntityKindName(entity),
                    getEntityKindName(it->second), next_storage->getName());
            continue;
        }
        bool shadowed = false;
        for (const auto & key : getClaimKeys(entity_name, entity))
        {
            if (auto it = inherited_claims.find(key); it != inherited_claims.end())
            {
                shadowed = true;
                if (!shadowed_entities.contains(entity_name))
                    LOG_INFO(log, "Workload entity '{}' is shadowed by '{}' of {} storage, both claim {}",
                        entity_name, it->second, next_storage->getName(), claimKeyToString(key));
                break;
            }
        }
        if (shadowed)
            new_shadowed_entities.insert(entity_name);
        else
        {
            if (shadowed_entities.contains(entity_name))
                LOG_INFO(log, "Workload entity '{}' is not shadowed any longer", entity_name);
            new_visible_entities.emplace(entity_name, entity);
        }
    }
    shadowed_entities = std::move(new_shadowed_entities);

    // Persisted data may form a parent cycle: its workloads are hidden
    for (const String & entity_name : findParentCycles(new_visible_entities))
    {
        new_visible_entities.erase(entity_name);
        new_conflicting_entities.insert(entity_name);
        if (!conflicting_entities.contains(entity_name))
            LOG_ERROR(log, "Workload entity '{}' of the {} storage is hidden: it is on a parent cycle. Change its parent.",
                entity_name, getName());
    }
    conflicting_entities = std::move(new_conflicting_entities);

    // A workload whose parent is not a visible workload is hidden as well (until a fixpoint), so that the visible
    // entities never have a workload without its parent.
    std::unordered_set<String> new_orphaned_entities;
    for (bool changed = true; changed;)
    {
        changed = false;
        for (auto it = new_visible_entities.begin(); it != new_visible_entities.end();)
        {
            const auto & [entity_name, entity] = *it;
            auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(entity.get());
            if (!workload || !workload->hasParent())
            {
                ++it;
                continue;
            }
            String parent = workload->getWorkloadParent();
            auto parent_it = new_visible_entities.find(parent);
            if (parent_it != new_visible_entities.end() && typeid_cast<ASTCreateWorkloadQuery *>(parent_it->second.get()))
            {
                ++it;
                continue;
            }
            new_orphaned_entities.insert(entity_name);
            if (!orphaned_entities.contains(entity_name))
                LOG_INFO(log, "Workload entity '{}' is hidden in {} storage: its parent '{}' is not a visible workload", entity_name, getName(), parent);
            it = new_visible_entities.erase(it);
            changed = true;
        }
    }
    orphaned_entities = std::move(new_orphaned_entities);

    addDerivedEntities(new_visible_entities);

    // Fill vector of `changes` based on difference between current and new visible entities
    std::vector<EntityChange> changes;
    for (const auto & [entity_name, entity] : visible_entities)
    {
        if (auto it = new_visible_entities.find(entity_name); it != new_visible_entities.end())
        {
            if (!entityEquals(entity, it->second))
                changes.emplace_back(entity_name, entity, it->second);
        }
        else
            changes.emplace_back(entity_name, entity, ASTPtr{});
    }
    for (const auto & [entity_name, entity] : new_visible_entities)
    {
        if (!visible_entities.contains(entity_name))
            changes.emplace_back(entity_name, ASTPtr{}, entity);
    }

    // Sort `changes` to respect consistency of references and apply them one by one
    std::vector<Event> tx;
    for (const auto & change : topologicallySortedChanges(changes))
    {
        for (const auto & event : change.toEvents())
        {
            applyEvent(lock, event);
            tx.push_back(event);
        }
    }

    updateRoleResources();

    // Notify subscribers
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

void WorkloadEntityStorageBase::setOwnEntities(const std::vector<std::pair<String, ASTPtr>> & raw_new_entities, OnOwnConflict on_conflict)
{
    std::unordered_map<String, ASTPtr> new_own_entities;
    for (const auto & [entity_name, create_query] : raw_new_entities)
    {
        // Persisted data written by older versions may use a reserved name: such an entity is hidden, not rejected
        if (!(on_conflict == OnOwnConflict::Hide && isReservedEntityName(entity_name)))
            checkUserEntityName(entity_name);
        // Two definitions of one name conflict, as they share the name claim key
        auto [it, inserted] = new_own_entities.try_emplace(entity_name, normalizeCreateWorkloadEntityQuery(*create_query));
        if (!inserted)
        {
            if (on_conflict == OnOwnConflict::Throw)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' is defined more than once", entity_name);
            LOG_ERROR(log, "Workload entity '{}' of the {} storage is defined more than once, the first definition is used", entity_name, getName());
        }
    }

    // Conflicts between own entities reject the load, or are resolved in `updateVisibleEntities` for persisted
    // data written earlier. Overlap with inherited entities is resolved by shadowing.
    if (on_conflict == OnOwnConflict::Throw)
    {
        std::map<ClaimKey, String> own_claims;
        for (const auto & [entity_name, entity] : new_own_entities)
        {
            for (auto & key : getClaimKeys(entity_name, entity))
            {
                if (auto [it, inserted] = own_claims.emplace(key, entity_name); !inserted)
                    throwClaimConflict(key, it->second);
            }
        }
    }

    std::unique_lock lock(mutex);

    // Reject cost-unit changes from config/Keeper refresh just like the SQL path does. The
    // scheduler hierarchy is built per unit, so silently swapping units would leave classifiers
    // handing back `ResourceLink`s for the wrong pointer field.
    for (const auto & [entity_name, entity] : new_own_entities)
    {
        if (auto it = own_entities.find(entity_name); it != own_entities.end() && resourceUnitChanged(it->second, entity))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cost unit of resource '{}' cannot be changed; drop and recreate the resource instead.",
                entity_name);
    }

    // The configuration rejects broken references. Persisted data is loaded as is: a workload with a dangling or
    // wrong-kind parent or on a parent cycle is hidden, a `FOR` clause naming no resource applies to no resource.
    if (on_conflict == OnOwnConflict::Throw)
        checkLoadedReferences(new_own_entities);

    // Validate new/changed workloads' setting VALUES on the config/Keeper/disk load path (which
    // bypasses storeEntity) — otherwise a bad value (e.g. scheduler = 'bogus', a negative weight, or
    // such a value inside a `... FOR <resource>` clause) is accepted here and only surfaces later,
    // when the scheduler node is built. Pass `throw_on_unknown_setting = false` so this LOAD path
    // stays forward-compatible and no stricter than the runtime parser in
    // `WorkloadResourceManager::NodeInfo` (which also uses `false`): an unknown setting NAME written
    // by a newer node must not make an older node reject the whole entity. Value checks (scheduler
    // algorithm, non-negative numerics) fire regardless of that flag, so a genuinely bad value is
    // still rejected. Both the base group and each per-resource group are checked. Only new/changed
    // entities are checked (like the cost-unit check above), so a pre-existing entity is not
    // re-validated on every refresh.
    for (const auto & [entity_name, entity] : new_own_entities)
    {
        if (auto it = own_entities.find(entity_name); it != own_entities.end() && entityEquals(it->second, entity))
            continue;
        if (auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(entity.get()))
        {
            WorkloadSettings validator;
            validator.initFromChanges(workload->changes, /*resource_name=*/{}, /*throw_on_unknown_setting=*/false);
            std::unordered_set<String> validated_resources;
            for (const auto & setting_change : workload->changes)
            {
                if (!setting_change.resource.empty() && validated_resources.insert(setting_change.resource).second)
                {
                    WorkloadSettings resource_validator;
                    resource_validator.initFromChanges(workload->changes, setting_change.resource, /*throw_on_unknown_setting=*/false);
                }
            }
        }
    }

    replaceOwnEntities(lock, std::move(new_own_entities));
}

void WorkloadEntityStorageBase::replaceOwnEntities(std::unique_lock<std::recursive_mutex> & lock, std::unordered_map<String, ASTPtr> new_own_entities)
{
    own_entities = std::move(new_own_entities);
    updateVisibleEntities(lock);
}

void WorkloadEntityStorageBase::applyEvent(
    std::unique_lock<std::recursive_mutex> &,
    const Event & event)
{
    if (event.entity) // CREATE || CREATE OR REPLACE
    {
        LOG_DEBUG(log, "Create or replace workload entity: {}", event.entity->formatForLogging());

        // Remove references of a replaced entity (only for CREATE OR REPLACE)
        if (auto it = visible_entities.find(event.name); it != visible_entities.end())
            removeReferences(it->second);

        // Insert references of created entity
        insertReferences(event.entity);

        // Store in memory
        visible_entities[event.name] = event.entity;
    }
    else // DROP
    {
        auto it = visible_entities.find(event.name);
        chassert(it != visible_entities.end());

        LOG_DEBUG(log, "Drop workload entity: {}", event.name);

        // Clean up references
        removeReferences(it->second);

        // Remove from memory
        visible_entities.erase(it);
    }
}

std::vector<std::pair<String, ASTPtr>> WorkloadEntityStorageBase::getAllEntities() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_entities;
    all_entities.reserve(visible_entities.size());
    std::copy(visible_entities.begin(), visible_entities.end(), std::back_inserter(all_entities));
    return all_entities;
}

bool WorkloadEntityStorageBase::isIndirectlyReferenced(const String & target, const String & source) const
{
    // Walk the references of both the own and the inherited entity of every name, not only of visible ones:
    // a hidden own entity becomes visible again later, together with its references
    std::queue<String> bfs;
    std::unordered_set<String> visited;
    visited.insert(source);
    bfs.push(source);
    while (!bfs.empty())
    {
        String current = bfs.front();
        bfs.pop();
        if (current == target)
            return true;
        for (const auto * layer_entities : {&own_entities, &inherited_entities})
        {
            auto it = layer_entities->find(current);
            if (it == layer_entities->end())
                continue;
            forEachReference(it->second, [&](const String & next, const String &, ReferenceType)
            {
                if (visited.insert(next).second)
                    bfs.push(next);
            });
        }
    }
    return false;
}

void WorkloadEntityStorageBase::insertReferences(const ASTPtr & entity)
{
    if (!entity)
        return;
    forEachReference(entity,
        [this] (const String & target, const String & source, ReferenceType)
        {
            references[target].insert(source);
        });
}

void WorkloadEntityStorageBase::removeReferences(const ASTPtr & entity)
{
    if (!entity)
        return;
    forEachReference(entity,
        [this] (const String & target, const String & source, ReferenceType)
        {
            references[target].erase(source);
            if (references[target].empty())
                references.erase(target);
        });
}

std::vector<WorkloadEntityStorageBase::Event> WorkloadEntityStorageBase::orderEntities(
    const std::unordered_map<String, ASTPtr> & all_entities,
    std::optional<Event> change)
{
    std::vector<Event> result;

    std::unordered_map<String, ASTPtr> workloads;
    for (const auto & [entity_name, ast] : all_entities)
    {
        if (typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
        {
            if (change && change->name == entity_name)
                continue; // Skip this workload if it is removed or updated
            workloads.emplace(entity_name, ast);
        }
        else if (typeid_cast<ASTCreateResourceQuery *>(ast.get()))
        {
            if (change && change->name == entity_name)
                continue; // Skip this resource if it is removed or updated
            // Resources should go first because workloads could reference them
            result.emplace_back(WorkloadEntityType::Resource, entity_name, ast);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid workload entity type '{}'", ast->getID());
    }

    // Introduce new entity described by `change`
    if (change && change->entity)
    {
        if (change->type == WorkloadEntityType::Workload)
            workloads.emplace(change->name, change->entity);
        else if (change->type == WorkloadEntityType::Resource)
            result.emplace_back(WorkloadEntityType::Resource, change->name, change->entity);
    }

    // Workloads should go in an order such that children are enlisted only after its parent
    for (auto & [entity_name, ast] : topologicallySortedWorkloads(workloads))
        result.emplace_back(WorkloadEntityType::Workload, entity_name, ast);

    return result;
}

String WorkloadEntityStorageBase::serializeOwnEntities(std::optional<Event> change)
{
    std::unique_lock<std::recursive_mutex> lock;
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

std::vector<std::pair<String, ASTPtr>> WorkloadEntityStorageBase::parseEntitiesFromString(const String & data, LoggerPtr log)
{
    std::vector<std::pair<String, ASTPtr>> result;

    // Parse multiple SQL statements from data
    ASTs queries;
    ParserCreateWorkloadEntity parser;
    const char * begin = data.data(); /// begin of current query
    const char * pos = begin; /// parser moves pos from begin to the end of current query
    const char * end = begin + data.size();
    while (pos < end)
    {
        queries.emplace_back(parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
        while (isWhitespaceASCII(*pos) || *pos == ';')
            ++pos;
    }

    /// Parse each query and extract entity name
    for (const auto & query : queries)
    {
        LOG_TRACE(log, "Parsed entity definition: {}", query->formatForLogging());
        String entity_name;
        if (auto * create_workload_query = query->as<ASTCreateWorkloadQuery>())
            entity_name = create_workload_query->getWorkloadName();
        else if (auto * create_resource_query = query->as<ASTCreateResourceQuery>())
            entity_name = create_resource_query->getResourceName();
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid workload entity query: {}", query->getID());

        result.emplace_back(entity_name, query);
    }

    return result;
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

        for (const auto & [entity_name, ast] : it->second)
        {
            if (getEntityType(ast) != entity_type)
                continue;
            /// Warn when an entity being backed up references one defined in the server configuration: such
            /// config-defined entities are not part of the backup (they belong to the server config), so a
            /// restore on a server that lacks them in its config will fail. Evaluated only over the entities
            /// selected for this call (past the entity_type filter above), so a resources-only backup does not
            /// warn about workloads and vice versa. `inherited_entities` holds the config-defined entities.
            forEachReference(ast, [&](const String & target, const String & source, ReferenceType type)
            {
                if (!it->second.contains(target) && inherited_entities.contains(target))
                    LOG_WARNING(log, "Backed up workload entity '{}' references {} '{}' defined in the server "
                        "configuration, which is not included in the backup; restoring it requires '{}' to "
                        "already exist on the destination server.",
                        source, type == ReferenceType::ForResource ? "resource" : "parent workload", target, target);
            });
            backup_entries.emplace_back(
                escapeForFileName(entity_name) + ".sql",
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

    std::vector<std::pair<String, ASTPtr>> parsed_entities;
    for (const auto & filename : filenames)
    {
        String filepath = data_path_in_backup_fs / filename;
        auto in = backup->readFile(filepath);
        String statement_def;
        readStringUntilEOF(statement_def, *in);

        for (auto & name_and_ast : parseEntitiesFromString(statement_def, log))
        {
            /// Reject a definition whose kind does not match the system table being restored. The two system
            /// tables carry independent access checks (system.workloads requires CREATE_WORKLOAD, system.resources
            /// requires CREATE_RESOURCE; see RestorerFromBackup::checkAccessForObjectsFoundInBackup), so accepting
            /// a WORKLOAD from the resources directory (or vice versa) would bypass the access gate of the other
            /// kind and violate the system-table backup contract.
            if (getEntityType(name_and_ast.second) != entity_type)
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore workload entities: file {} defines an entity of a kind that does not match the "
                    "system table being restored",
                    String{data_path_in_backup_fs / filename});
            parsed_entities.push_back(std::move(name_and_ast));
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
        for (auto & [name, ast] : parsed_entities)
            accumulated.emplace(name, ast);
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
    std::unordered_map<String, ASTPtr> to_restore;
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
            forEachReference(entry.second, [&](const String & target, const String & source, ReferenceType type)
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
                    type == ReferenceType::ForResource ? "resource" : "parent workload",
                    target);
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
