#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>

#include <boost/container/flat_set.hpp>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateWorkloadQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int WORKLOAD_ENTITY_ALREADY_EXISTS;
    extern const int UNKNOWN_WORKLOAD_ENTITY;
}

namespace
{

ASTPtr normalizeCreateWorkloadEntityQuery(const IAST & create_query, const ContextPtr & context)
{
    UNUSED(context);
    auto ptr = create_query.clone();
    auto & res = typeid_cast<ASTCreateWorkloadQuery &>(*ptr); // TODO(serxa): we should also check for ASTCreateResourceQuery
    res.if_not_exists = false;
    res.or_replace = false;
    return ptr;
}

}

WorkloadEntityStorageBase::WorkloadEntityStorageBase(ContextPtr global_context_)
    : global_context(std::move(global_context_))
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
    std::lock_guard lock{mutex};
    auto it = entities.find(entity_name);
    if (it != entities.end())
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::WORKLOAD_ENTITY_ALREADY_EXISTS, "Workload entity '{}' already exists", entity_name);
        else if (!replace_if_exists)
            return false;
    }

    bool stored = storeEntityImpl(
        current_context,
        entity_type,
        entity_name,
        create_entity_query,
        throw_if_exists,
        replace_if_exists,
        settings);

    if (stored)
        entities[entity_name] = create_entity_query;

    return stored;
}

bool WorkloadEntityStorageBase::removeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists)
{
    std::lock_guard lock(mutex);
    auto it = entities.find(entity_name);
    if (it == entities.end())
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_WORKLOAD_ENTITY, "Workload entity '{}' doesn't exist", entity_name);
        else
            return false;
    }

    bool removed = removeEntityImpl(
        current_context,
        entity_type,
        entity_name,
        throw_if_not_exists);

    if (removed)
        entities.erase(entity_name);

    return removed;
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

    std::lock_guard lock(mutex);
    entities = std::move(normalized_entities);
}

std::vector<std::pair<String, ASTPtr>> WorkloadEntityStorageBase::getAllEntities() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_entities;
    all_entities.reserve(entities.size());
    std::copy(entities.begin(), entities.end(), std::back_inserter(all_entities));
    return all_entities;
}

void WorkloadEntityStorageBase::setEntity(const String & entity_name, const IAST & create_entity_query)
{
    std::lock_guard lock(mutex);
    entities[entity_name] = normalizeCreateWorkloadEntityQuery(create_entity_query, global_context);
}

void WorkloadEntityStorageBase::removeEntity(const String & entity_name)
{
    std::lock_guard lock(mutex);
    entities.erase(entity_name);
}

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
