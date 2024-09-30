#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>

#include <Common/Scheduler/SchedulingSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>

#include <boost/container/flat_set.hpp>
#include <boost/range/algorithm/copy.hpp>

#include <mutex>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int WORKLOAD_ENTITY_ALREADY_EXISTS;
    extern const int UNKNOWN_WORKLOAD_ENTITY;
    extern const int LOGICAL_ERROR;
}

namespace
{

ASTPtr normalizeCreateWorkloadEntityQuery(const IAST & create_query, const ContextPtr & context)
{
    UNUSED(context);
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

WorkloadEntityType getEntityType(const ASTPtr & ptr)
{
    if (auto * res = typeid_cast<ASTCreateWorkloadQuery *>(ptr.get()))
        return WorkloadEntityType::Workload;
    if (auto * res = typeid_cast<ASTCreateResourceQuery *>(ptr.get()))
        return WorkloadEntityType::Resource;
    chassert(false);
    return WorkloadEntityType::MAX;
}

enum class ReferenceType
{
    Parent, ForResource
};

void forEachReference(const ASTPtr & source_entity, std::function<void(String, String, ReferenceType)> func)
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
    if (auto * res = typeid_cast<ASTCreateResourceQuery *>(source_entity.get()))
    {
        // RESOURCE has no references to be validated, we allow mentioned disks to be created later
    }
}

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

std::vector<std::pair<String, ASTPtr>> topologicallySortedWorkloads(const std::unordered_map<String, ASTPtr> & workloads)
{
    std::vector<std::pair<String, ASTPtr>> sorted_workloads;
    std::unordered_set<String> visited;
    for (const auto & [name, ast] : workloads)
        topologicallySortedWorkloadsImpl(name, ast, workloads, visited, sorted_workloads);
    return sorted_workloads;
}

}

WorkloadEntityStorageBase::WorkloadEntityStorageBase(ContextPtr global_context_)
    : handlers(std::make_shared<Handlers>())
    , global_context(std::move(global_context_))
{}

ASTPtr WorkloadEntityStorageBase::get(const String & entity_name) const
{
    std::lock_guard lock(mutex);

    auto it = entities.find(entity_name);
    if (it == entities.end())
        throw Exception(ErrorCodes::UNKNOWN_WORKLOAD_ENTITY,
            "The workload entity name '{}' is not saved",
            entity_name);

    return it->second;
}

ASTPtr WorkloadEntityStorageBase::tryGet(const std::string & entity_name) const
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

std::vector<std::string> WorkloadEntityStorageBase::getAllEntityNames() const
{
    std::vector<std::string> entity_names;

    std::lock_guard lock(mutex);
    entity_names.reserve(entities.size());

    for (const auto & [name, _] : entities)
        entity_names.emplace_back(name);

    return entity_names;
}

std::vector<std::string> WorkloadEntityStorageBase::getAllEntityNames(WorkloadEntityType entity_type) const
{
    std::vector<std::string> entity_names;

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

    auto * workload = typeid_cast<ASTCreateWorkloadQuery *>(create_entity_query.get());
    if (workload)
    {
        if (entity_name == workload->getWorkloadParent())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Self-referencing workloads are not allowed.");
    }

    std::unique_lock lock{mutex};

    create_entity_query = normalizeCreateWorkloadEntityQuery(*create_entity_query, global_context);

    if (auto it = entities.find(entity_name); it != entities.end())
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::WORKLOAD_ENTITY_ALREADY_EXISTS, "Workload entity '{}' already exists", entity_name);
        else if (!replace_if_exists)
            return false;
    }

    std::optional<String> new_root_name;

    // Validate workload
    if (workload)
    {
        if (!workload->hasParent())
        {
            if (!root_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second root is not allowed. You should probably add 'PARENT {}' clause.", root_name);
            new_root_name = workload->getWorkloadName();
        }

        SchedulingSettings validator;
        validator.updateFromChanges(workload->changes);
    }

    forEachReference(create_entity_query,
        [this, workload] (const String & target, const String & source, ReferenceType type)
        {
            if (auto it = entities.find(target); it == entities.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload entity '{}' references another workload entity '{}' that doesn't exist", source, target);

            // Validate that we could parse the settings for specific resource
            if (type == ReferenceType::ForResource)
            {
                if (typeid_cast<ASTCreateResourceQuery *>(entities[target].get()) == nullptr)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Workload settings should reference resource in FOR clause, not '{}'.", target);

                SchedulingSettings validator;
                validator.updateFromChanges(workload->changes, target);
            }
        });

    bool stored = storeEntityImpl(
        current_context,
        entity_type,
        entity_name,
        create_entity_query,
        throw_if_exists,
        replace_if_exists,
        settings);

    if (stored)
    {
        if (new_root_name)
            root_name = *new_root_name;
        forEachReference(create_entity_query,
            [this] (const String & target, const String & source, ReferenceType)
            {
                references[target].insert(source);
            });
        entities[entity_name] = create_entity_query;
        onEntityAdded(entity_type, entity_name, create_entity_query);
        unlockAndNotify(lock);
    }

    return stored;
}

bool WorkloadEntityStorageBase::removeEntity(
    const ContextPtr & current_context,
    WorkloadEntityType entity_type,
    const String & entity_name,
    bool throw_if_not_exists)
{
    std::unique_lock lock(mutex);
    auto it = entities.find(entity_name);
    if (it == entities.end())
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_WORKLOAD_ENTITY, "Workload entity '{}' doesn't exist", entity_name);
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

    bool removed = removeEntityImpl(
        current_context,
        entity_type,
        entity_name,
        throw_if_not_exists);

    if (removed)
    {
        if (entity_name == root_name)
            root_name.clear();
        forEachReference(it->second,
            [this] (const String & target, const String & source, ReferenceType)
            {
                references[target].erase(source);
                if (references[target].empty())
                    references.erase(target);
            });
        entities.erase(it);
        onEntityRemoved(entity_type, entity_name);

        unlockAndNotify(lock);
    }

    return removed;
}

scope_guard WorkloadEntityStorageBase::getAllEntitiesAndSubscribe(const OnChangedHandler & handler)
{
    scope_guard result;

    std::vector<Event> current_state;
    {
        std::unique_lock lock{mutex};
        chassert(queue.empty());
        makeEventsForAllEntities(lock);
        current_state = std::move(queue);

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

void WorkloadEntityStorageBase::onEntityAdded(WorkloadEntityType entity_type, const String & entity_name, const ASTPtr & new_entity)
{
    queue.push_back(Event{.type = entity_type, .name = entity_name, .entity = new_entity});
}

void WorkloadEntityStorageBase::onEntityUpdated(WorkloadEntityType entity_type, const String & entity_name, const ASTPtr & changed_entity)
{
    queue.push_back(Event{.type = entity_type, .name = entity_name, .entity = changed_entity});
}

void WorkloadEntityStorageBase::onEntityRemoved(WorkloadEntityType entity_type, const String & entity_name)
{
    queue.push_back(Event{.type = entity_type, .name = entity_name, .entity = {}});
}

void WorkloadEntityStorageBase::unlockAndNotify(std::unique_lock<std::recursive_mutex> & mutex_lock)
{
    /// Only one thread can send notification at any time, that is why we need `mutex_lock`
    if (!queue.empty())
    {
        auto events = std::move(queue);

        std::vector<OnChangedHandler> current_handlers;
        {
            std::lock_guard handlers_lock{handlers->mutex};
            boost::range::copy(handlers->list, std::back_inserter(current_handlers));
        }

        mutex_lock.unlock();

        for (const auto & handler : current_handlers)
        {
            try
            {
                handler(events);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

std::unique_lock<std::recursive_mutex> WorkloadEntityStorageBase::getLock() const
{
    return std::unique_lock{mutex};
}

void WorkloadEntityStorageBase::setAllEntities(const std::vector<std::pair<String, ASTPtr>> & new_entities)
{
    std::unordered_map<String, ASTPtr> normalized_entities;
    for (const auto & [entity_name, create_query] : new_entities)
        normalized_entities[entity_name] = normalizeCreateWorkloadEntityQuery(*create_query, global_context);

    // TODO(serxa): do validation and throw LOGICAL_ERROR if failed

    std::unique_lock lock(mutex);
    chassert(entities.empty());
    entities = std::move(normalized_entities);
    for (const auto & [entity_name, entity] : entities)
    {
        forEachReference(entity,
            [this] (const String & target, const String & source, ReferenceType)
            {
                references[target].insert(source);
            });
    }


    // Quick check to avoid extra work
    {
        std::lock_guard lock2(handlers->mutex);
        if (handlers->list.empty())
            return;
    }

    makeEventsForAllEntities(lock);
    unlockAndNotify(lock);
}

void WorkloadEntityStorageBase::makeEventsForAllEntities(std::unique_lock<std::recursive_mutex> &)
{
    std::unordered_map<String, ASTPtr> workloads;
    std::unordered_map<String, ASTPtr> resources;
    for (auto & [entity_name, ast] : entities)
    {
        if (typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
            workloads.emplace(entity_name, ast);
        else if (typeid_cast<ASTCreateResourceQuery *>(ast.get()))
            resources.emplace(entity_name, ast);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid workload entity type '{}'", ast->getID());
    }

    for (auto & [entity_name, ast] : topologicallySortedWorkloads(workloads))
        onEntityAdded(WorkloadEntityType::Workload, entity_name, ast);

    for (auto & [entity_name, ast] : resources)
        onEntityAdded(WorkloadEntityType::Resource, entity_name, ast);
}

std::vector<std::pair<String, ASTPtr>> WorkloadEntityStorageBase::getAllEntities() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_entities;
    all_entities.reserve(entities.size());
    std::copy(entities.begin(), entities.end(), std::back_inserter(all_entities));
    return all_entities;
}

// TODO(serxa): add notifications or remove this function
void WorkloadEntityStorageBase::removeAllEntitiesExcept(const Strings & entity_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{entity_names_to_keep.begin(), entity_names_to_keep.end()};
    std::lock_guard lock(mutex);
    for (auto it = entities.begin(); it != entities.end();)
    {
        auto current = it++;
        if (!names_set_to_keep.contains(current->first))
            entities.erase(current);
    }
}

}
