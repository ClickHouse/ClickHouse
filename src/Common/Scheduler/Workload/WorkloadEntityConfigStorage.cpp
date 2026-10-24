#include <Common/Scheduler/Workload/WorkloadEntityConfigStorage.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

WorkloadEntityConfigStorage::WorkloadEntityConfigStorage(const ContextPtr & global_context_)
    : WorkloadEntityStorageBase(global_context_)
{
}

void WorkloadEntityConfigStorage::loadEntities(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Refreshing workload entities from configuration");
    ASTs new_entities = parseEntitiesFromString(config.getString("resources_and_workloads", ""), log);
    setOwnEntities(new_entities);
    LOG_DEBUG(log, "Loaded {} workload entities from configuration", new_entities.size());
}

WorkloadEntityConfigStorage::OperationResult WorkloadEntityConfigStorage::storeEntityImpl(
    const ContextPtr &,
    WorkloadEntityType,
    const String &,
    ASTPtr,
    bool,
    bool,
    const Settings &)
{
    // Config storage is read-only - entities come from config, not SQL. This function should not be called
    return OperationResult::Failed;
}

WorkloadEntityConfigStorage::OperationResult WorkloadEntityConfigStorage::removeEntityImpl(
    const ContextPtr &,
    WorkloadEntityType,
    const String &,
    bool)
{
    // Config storage is read-only - entities come from config, not SQL. This function should not be called
    return OperationResult::Failed;
}

}
