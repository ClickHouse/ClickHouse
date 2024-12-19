#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>

#include <Common/Scheduler/SchedulingSettings.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromString.h>

#include <boost/container/flat_set.hpp>
#include <boost/range/algorithm/copy.hpp>

#include <mutex>
#include <queue>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
        auto parent_iter = workloads.find(parent);
        if (parent_iter == workloads.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Workload metadata inconsistency: Workload '{}' parent '{}' does not exist. This must be fixed manually.", name, parent);
        topologicallySortedWorkloadsImpl(parent, parent_iter->second, workloads, visited, sorted_workloads);
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
            // If type changed, we have to remove an old entity and add a new one
            if (type_before != type_after)
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
                        // If target is creating, it should be created first.
                        // (But if target is updating, there is no dependency).
                        if (!target_change.before)
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

WorkloadEntityStorageBase::WorkloadEntityStorageBase(ContextPtr global_context_)
    : handlers(std::make_shared<Handlers>())
    , global_context(std::move(global_context_))
    , log{getLogger("WorkloadEntityStorage")} // could be overridden in derived class
{}

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

    auto it = entities.find(entity_name);
    if (it == entities.end())
        return nullptr;

    return it->second;
}

bool WorkloadEntityStorageBase::has(const String & entity_name) const
{
    return tryGet(entity_name) != nullptr;
}

std::vector<String> WorkloadEntityStorageBase::getAllEntityNames() const
{
    std::vector<String> entity_names;

    std::lock_guard lock(mutex);
    entity_names.reserve(entities.size());

    for (const auto & [name, _] : entities)
        entity_names.emplace_back(name);

    return entity_names;
}

std::vector<String> WorkloadEntityStorageBase::getAllEntityNames(WorkloadEntityType entity_type) const
{
    std::vector<String> entity_names;

    std::lock_guard lock(mutex);
    for (const auto & [name, entity] : entities)
    {
        if (getEntityType(entity) == entity_type)
            entity_names.emplace_back(name);
    }

    return entity_names;
}

bool WorkloadEntityStorageBase::empty() const
{
    std::lock_guard lock(mutex);
    return entities.empty();
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
    if (entity_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity name should not be empty.");

    create_entity_query = normalizeCreateWorkloadEntityQuery(*create_entity_query);
    auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(create_entity_query.get());
    auto * resource = typeid_cast<ASTCreateResourceQuery *>(create_entity_query.get());

    while (true)
    {
        std::unique_lock lock{mutex};

        ASTPtr old_entity; // entity to be REPLACED
        if (auto it = entities.find(entity_name); it != entities.end())
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists", entity_name);
            else if (!replace_if_exists)
                return false;
            else
                old_entity = it->second;
        }

        // Validate CREATE OR REPLACE
        if (old_entity)
        {
            auto * old_workload = typeid_cast<ASTCreateWorkloadQuery *>(old_entity.get());
            auto * old_resource = typeid_cast<ASTCreateResourceQuery *>(old_entity.get());
            if (workload && !old_workload)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists, but it is not a workload", entity_name);
            if (resource && !old_resource)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' already exists, but it is not a resource", entity_name);
            if (workload && !old_workload->hasParent() && workload->hasParent())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "It is not allowed to remove root workload");
        }

        // Validate workload
        if (workload)
        {
            if (!workload->hasParent())
            {
                if (!root_name.empty() && root_name != workload->getWorkloadName())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second root is not allowed. You should probably add 'PARENT {}' clause.", root_name);
            }

            SchedulingSettings validator;
            validator.updateFromChanges(workload->changes);
        }

        forEachReference(create_entity_query,
            [this, workload] (const String & target, const String & source, ReferenceType type)
            {
                if (auto it = entities.find(target); it == entities.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' references another workload entity '{}' that doesn't exist", source, target);

                switch (type)
                {
                    case ReferenceType::Parent:
                    {
                        if (typeid_cast<ASTCreateWorkloadQuery *>(entities[target].get()) == nullptr)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload parent should reference another workload, not '{}'.", target);
                        break;
                    }
                    case ReferenceType::ForResource:
                    {
                        if (typeid_cast<ASTCreateResourceQuery *>(entities[target].get()) == nullptr)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload settings should reference resource in FOR clause, not '{}'.", target);

                        // Validate that we could parse the settings for specific resource
                        SchedulingSettings validator;
                        validator.updateFromChanges(workload->changes, target);
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
            Event event{entity_type, entity_name, create_entity_query};
            applyEvent(lock, event);
            unlockAndNotify(lock, {std::move(event)});
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
        auto it = entities.find(entity_name);
        if (it == entities.end())
        {
            if (throw_if_not_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' doesn't exist", entity_name);
            else
                return false;
        }

        if (auto reference_it = references.find(entity_name); reference_it != references.end())
        {
            String names;
            for (const String & name : reference_it->second)
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
            Event event{entity_type, entity_name, {}};
            applyEvent(lock, event);
            unlockAndNotify(lock, {std::move(event)});
        }

        return result == OperationResult::Ok;
    }
}

scope_guard WorkloadEntityStorageBase::getAllEntitiesAndSubscribe(const OnChangedHandler & handler)
{
    scope_guard result;

    std::vector<Event> current_state;
    {
        std::lock_guard lock{mutex};
        current_state = orderEntities(entities);

        std::lock_guard lock2{handlers->mutex};
        handlers->list.push_back(handler);
        auto handler_it = std::prev(handlers->list.end());
        result = [my_handlers = handlers, handler_it]
        {
            std::lock_guard lock3{my_handlers->mutex};
            my_handlers->list.erase(handler_it);
        };
    }

    // When you subscribe you get all the entities back to your handler immediately if already loaded, or later when loaded
    handler(current_state);

    return result;
}

void WorkloadEntityStorageBase::unlockAndNotify(
    std::unique_lock<std::recursive_mutex> & lock,
    std::vector<Event> tx)
{
    if (tx.empty())
        return;

    std::vector<OnChangedHandler> current_handlers;
    {
        std::lock_guard handlers_lock{handlers->mutex};
        boost::range::copy(handlers->list, std::back_inserter(current_handlers));
    }

    lock.unlock();

    for (const auto & handler : current_handlers)
    {
        try
        {
            handler(tx);
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

void WorkloadEntityStorageBase::setAllEntities(const std::vector<std::pair<String, ASTPtr>> & raw_new_entities)
{
    std::unordered_map<String, ASTPtr> new_entities;
    for (const auto & [entity_name, create_query] : raw_new_entities)
        new_entities[entity_name] = normalizeCreateWorkloadEntityQuery(*create_query);

    std::unique_lock lock(mutex);

    // Fill vector of `changes` based on difference between current `entities` and `new_entities`
    std::vector<EntityChange> changes;
    for (const auto & [entity_name, entity] : entities)
    {
        if (auto it = new_entities.find(entity_name); it != new_entities.end())
        {
            if (!entityEquals(entity, it->second))
            {
                changes.emplace_back(entity_name, entity, it->second); // Update entities that are present in both `new_entities` and `entities`
                LOG_TRACE(log, "Workload entity {} was updated", entity_name);
            }
            else
                LOG_TRACE(log, "Workload entity {} is the same", entity_name);
        }
        else
        {
            changes.emplace_back(entity_name, entity, ASTPtr{}); // Remove entities that are not present in `new_entities`
            LOG_TRACE(log, "Workload entity {} was dropped", entity_name);
        }
    }
    for (const auto & [entity_name, entity] : new_entities)
    {
        if (!entities.contains(entity_name))
        {
            changes.emplace_back(entity_name, ASTPtr{}, entity); // Create entities that are only present in `new_entities`
            LOG_TRACE(log, "Workload entity {} was created", entity_name);
        }
    }

    // Sort `changes` to respect consistency of references and apply them one by one.
    std::vector<Event> tx;
    for (const auto & change : topologicallySortedChanges(changes))
    {
        for (const auto & event : change.toEvents())
        {
            // TODO(serxa): do validation and throw LOGICAL_ERROR if failed
            applyEvent(lock, event);
            tx.push_back(event);
        }
    }

    // Notify subscribers
    unlockAndNotify(lock, tx);
}

void WorkloadEntityStorageBase::applyEvent(
    std::unique_lock<std::recursive_mutex> &,
    const Event & event)
{
    if (event.entity) // CREATE || CREATE OR REPLACE
    {
        LOG_DEBUG(log, "Create or replace workload entity: {}", serializeAST(*event.entity));

        auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(event.entity.get());

        // Validate workload
        if (workload && !workload->hasParent())
            root_name = workload->getWorkloadName();

        // Remove references of a replaced entity (only for CREATE OR REPLACE)
        if (auto it = entities.find(event.name); it != entities.end())
            removeReferences(it->second);

        // Insert references of created entity
        insertReferences(event.entity);

        // Store in memory
        entities[event.name] = event.entity;
    }
    else // DROP
    {
        auto it = entities.find(event.name);
        chassert(it != entities.end());

        LOG_DEBUG(log, "Drop workload entity: {}", event.name);

        if (event.name == root_name)
            root_name.clear();

        // Clean up references
        removeReferences(it->second);

        // Remove from memory
        entities.erase(it);
    }
}

std::vector<std::pair<String, ASTPtr>> WorkloadEntityStorageBase::getAllEntities() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_entities;
    all_entities.reserve(entities.size());
    std::copy(entities.begin(), entities.end(), std::back_inserter(all_entities));
    return all_entities;
}

bool WorkloadEntityStorageBase::isIndirectlyReferenced(const String & target, const String & source)
{
    std::queue<String> bfs;
    std::unordered_set<String> visited;
    visited.insert(target);
    bfs.push(target);
    while (!bfs.empty())
    {
        String current = bfs.front();
        bfs.pop();
        if (current == source)
            return true;
        if (auto it = references.find(current); it != references.end())
        {
            for (const String & node : it->second)
            {
                if (visited.contains(node))
                    continue;
                visited.insert(node);
                bfs.push(node);
            }
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

String WorkloadEntityStorageBase::serializeAllEntities(std::optional<Event> change)
{
    std::unique_lock<std::recursive_mutex> lock;
    auto ordered_entities = orderEntities(entities, change);
    WriteBufferFromOwnString buf;
    for (const auto & event : ordered_entities)
    {
        formatAST(*event.entity, buf, false, true);
        buf.write(";\n", 2);
    }
    return buf.str();
}

}
