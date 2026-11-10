#include <Common/Scheduler/Workload/WorkloadEntityImplicitStorage.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <initializer_list>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Name of the implicit workload. It is the default value of the `workload` query setting.
constexpr std::string_view DEFAULT_WORKLOAD_NAME = "default";

/// Name of the server setting that defines the implicit entity
std::string_view getImplicitEntitySetting(const String & entity_name)
{
    if (entity_name == IMPLICIT_CPU_RESOURCE_NAME)
        return "workloads_respect_server_cpu_limit";
    if (entity_name == IMPLICIT_MEMORY_RESOURCE_NAME)
        return "workloads_respect_server_memory_limit";
    return "implicit_default_workload";
}

/// Builds a normalized `CREATE RESOURCE` query, the same as the parser produces
ASTPtr makeResource(std::string_view resource_name, std::initializer_list<ResourceAccessMode> modes)
{
    auto query = make_intrusive<ASTCreateResourceQuery>();
    query->resource_name = make_intrusive<ASTIdentifier>(String(resource_name));
    query->children.push_back(query->resource_name);
    for (auto mode : modes)
        query->operations.push_back(ASTCreateResourceQuery::Operation{.mode = mode, .disk = std::nullopt});
    query->unit = query->operations.front().unit();
    return query;
}

/// Builds a normalized parentless `CREATE WORKLOAD` query. The root workload (empty name) has no name identifier.
ASTPtr makeWorkload(std::string_view workload_name, ASTCreateWorkloadQuery::SettingsChanges changes)
{
    auto query = make_intrusive<ASTCreateWorkloadQuery>();
    if (!workload_name.empty())
    {
        query->workload_name = make_intrusive<ASTIdentifier>(String(workload_name));
        query->children.push_back(query->workload_name);
    }
    query->changes = std::move(changes);
    return query;
}

/// The implicit entities defined by `config`
WorkloadEntities makeImplicitEntities(const ImplicitWorkloadEntitiesConfig & config)
{
    ASTCreateWorkloadQuery::SettingsChanges root_changes;
    if (config.respect_cpu_limit && config.cpu_slots != ImplicitWorkloadEntitiesConfig::unlimited)
        root_changes.emplace_back("max_concurrent_threads", Field(static_cast<UInt64>(config.cpu_slots)), "");
    if (config.respect_memory_limit && config.memory_bytes != ImplicitWorkloadEntitiesConfig::unlimited)
        root_changes.emplace_back("max_memory", Field(static_cast<UInt64>(config.memory_bytes)), "");

    WorkloadEntities entities;
    entities.emplace(WorkloadEntityKey{WorkloadEntityType::Workload, ""}, makeWorkload("", std::move(root_changes)));
    if (config.respect_cpu_limit)
        entities.emplace(WorkloadEntityKey{WorkloadEntityType::Resource, String(IMPLICIT_CPU_RESOURCE_NAME)},
            makeResource(IMPLICIT_CPU_RESOURCE_NAME, {ResourceAccessMode::MasterThread, ResourceAccessMode::WorkerThread}));
    if (config.respect_memory_limit)
        entities.emplace(WorkloadEntityKey{WorkloadEntityType::Resource, String(IMPLICIT_MEMORY_RESOURCE_NAME)},
            makeResource(IMPLICIT_MEMORY_RESOURCE_NAME, {ResourceAccessMode::MemoryReservation}));
    if (config.implicit_default_workload)
        entities.emplace(WorkloadEntityKey{WorkloadEntityType::Workload, String(DEFAULT_WORKLOAD_NAME)},
            makeWorkload(DEFAULT_WORKLOAD_NAME, {}));
    return entities;
}

}

WorkloadEntityImplicitStorage::WorkloadEntityImplicitStorage(const ContextPtr & global_context_, std::unique_ptr<IWorkloadEntityStorage> next_storage_)
    : WorkloadEntityStorageBase(global_context_, std::move(next_storage_))
{
    log = getLogger("WorkloadEntityImplicitStorage");
    if (!getNextStorage())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Implicit workload entity storage requires a next storage");

    // The root workload is provided from the start, before the first `updateImplicitEntities`
    auto lock = getLock();
    replaceOwnEntities(lock, makeImplicitEntities(config));
}

bool WorkloadEntityImplicitStorage::isReplicated() const
{
    return getNextStorage()->isReplicated();
}

String WorkloadEntityImplicitStorage::getReplicationID() const
{
    return getNextStorage()->getReplicationID();
}

void WorkloadEntityImplicitStorage::stopWatching()
{
    getNextStorage()->stopWatching();
}

void WorkloadEntityImplicitStorage::updateImplicitEntities(const ImplicitWorkloadEntitiesConfig & new_config)
{
    auto lock = getLock();
    config = new_config;
    replaceOwnEntities(lock, makeImplicitEntities(config));
}

bool WorkloadEntityImplicitStorage::storeEntity(
    const ContextPtr & current_context,
    WorkloadEntityType entity_type,
    const String & entity_name,
    ASTPtr create_entity_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    return getNextStorage()->storeEntity(current_context, entity_type, entity_name, create_entity_query, throw_if_exists, replace_if_exists, settings);
}

bool WorkloadEntityImplicitStorage::definesEntity(const String & entity_name, WorkloadEntityType entity_type) const
{
    auto lock = getLock();
    return makeImplicitEntities(config).contains({entity_type, entity_name});
}

bool WorkloadEntityImplicitStorage::removeEntity(
    const ContextPtr & current_context,
    WorkloadEntityType entity_type,
    const String & entity_name,
    bool throw_if_not_exists)
{
    // An entity stored in an inner layer, visible or shadowed, is dropped there
    if (getNextStorage()->removeEntity(current_context, entity_type, entity_name, /* throw_if_not_exists = */ false))
        return true;

    // An implicit entity cannot be dropped, even while shadowed
    if (definesEntity(entity_name, entity_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Workload entity '{}' cannot be dropped: it is an implicit entity defined by the server setting `{}`",
            entity_name, getImplicitEntitySetting(entity_name));

    if (throw_if_not_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' doesn't exist",
            entity_type == WorkloadEntityType::Resource ? "Resource" : "Workload", entity_name);
    return false;
}

void WorkloadEntityImplicitStorage::backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, WorkloadEntityType entity_type) const
{
    getNextStorage()->backup(backup_entries_collector, data_path_in_backup, entity_type);
}

void WorkloadEntityImplicitStorage::restore(RestorerFromBackup & restorer, const String & data_path_in_backup, WorkloadEntityType entity_type)
{
    getNextStorage()->restore(restorer, data_path_in_backup, entity_type);
}

WorkloadEntityStorageBase::OperationResult WorkloadEntityImplicitStorage::storeEntityImpl(
    const ContextPtr &, WorkloadEntityType, const String &, ASTPtr, bool, bool, const Settings &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Implicit workload entity storage does not store entities, they are forwarded to the next storage");
}

WorkloadEntityStorageBase::OperationResult WorkloadEntityImplicitStorage::removeEntityImpl(
    const ContextPtr &, WorkloadEntityType, const String &, bool)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Implicit workload entity storage does not remove entities, they are forwarded to the next storage");
}

}
