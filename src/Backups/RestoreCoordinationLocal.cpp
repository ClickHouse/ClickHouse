#include <Backups/RestoreCoordinationLocal.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/chrono_io.h>


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

void RestoreCoordinationLocal::addReplicatedTableDataPath(const String & /* host_id */,
                                                          const DatabaseAndTableName & table_name,
                                                          const String & table_zk_path,
                                                          const String & data_path_in_backup)
{
    std::lock_guard lock{mutex};
    auto it = replicated_tables_data_paths.find(table_zk_path);
    if (it == replicated_tables_data_paths.end())
    {
        ReplicatedTableDataPath new_info;
        new_info.table_name = table_name;
        new_info.data_path_in_backup = data_path_in_backup;
        replicated_tables_data_paths.emplace(table_zk_path, std::move(new_info));
        return;
    }
    else
    {
        auto & cur_info = it->second;
        if (table_name < cur_info.table_name)
        {
            cur_info.table_name = table_name;
            cur_info.data_path_in_backup = data_path_in_backup;
        }
    }
}

String RestoreCoordinationLocal::getReplicatedTableDataPath(const String & table_zk_path) const
{
    std::lock_guard lock{mutex};
    auto it = replicated_tables_data_paths.find(table_zk_path);
    if (it == replicated_tables_data_paths.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replicated data path is not set for zk_path={}", table_zk_path);
    return it->second.data_path_in_backup;
}

bool RestoreCoordinationLocal::startInsertingDataToPartitionInReplicatedTable(
    const String & /* host_id */, const DatabaseAndTableName & table_name, const String & table_zk_path, const String & partition_name)
{
    std::lock_guard lock{mutex};
    auto key = std::pair{table_zk_path, partition_name};
    auto it = replicated_tables_partitions.try_emplace(std::move(key), table_name).first;
    return it->second == table_name;
}

}
