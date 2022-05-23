#include <Backups/RestoreCoordinationLocal.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Interpreters/StorageID.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RestoreCoordinationLocal::RestoreCoordinationLocal()
    : log(&Poco::Logger::get("RestoreCoordination"))
{}

RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

bool RestoreCoordinationLocal::startCreatingTableInReplicatedDB(
    const String & /* host_id */,
    const String & /* database_name */,
    const String & /* database_zk_path */,
    const String & /* table_name */)
{
    return true;
}

void RestoreCoordinationLocal::finishCreatingTableInReplicatedDB(
    const String & /* host_id */,
    const String & database_name,
    const String & /* database_zk_path */,
    const String & table_name,
    const String & error_message)
{
    if (error_message.empty())
        LOG_TRACE(log, "Created table {}.{}", database_name, table_name);
    else
        LOG_TRACE(log, "Failed to created table {}.{}: {}", database_name, table_name, error_message);
}

/// Wait for another host to create a table in a replicated database.
void RestoreCoordinationLocal::waitForTableCreatedInReplicatedDB(
    const String & /* database_name */,
    const String & /* database_zk_path */,
    const String & /* table_name */,
    std::chrono::seconds /* timeout */)
{
}

void RestoreCoordinationLocal::finishRestoringMetadata(const String & /* host_id */, const String & error_message)
{
    LOG_TRACE(log, "Finished restoring metadata{}", (error_message.empty() ? "" : (" with error " + error_message)));
}

void RestoreCoordinationLocal::waitForAllHostsRestoredMetadata(const Strings & /* host_ids */, std::chrono::seconds /* timeout */) const
{
}

bool RestoreCoordinationLocal::startInsertingDataToPartitionInReplicatedTable(
    const String & /* host_id */, const StorageID & table_id, const String & table_zk_path, const String & partition_name)
{
    std::lock_guard lock{mutex};
    auto key = std::pair{table_zk_path, partition_name};
    auto it = replicated_tables_partitions.try_emplace(std::move(key), table_id).first;
    return it->second == table_id;
}

}
