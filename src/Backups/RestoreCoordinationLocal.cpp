#include <Backups/RestoreCoordinationLocal.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/chrono_io.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RestoreCoordinationLocal::RestoreCoordinationLocal()
    : log(&Poco::Logger::get("RestoreCoordinationLocal"))
{}

RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

bool RestoreCoordinationLocal::startCreatingTableInReplicatedDB(
    const String & /* host_id_ */,
    const String & /* database_name_ */,
    const String & /* database_zk_path_*/,
    const String & /* table_name_ */)
{
    return true;
}

void RestoreCoordinationLocal::finishCreatingTableInReplicatedDB(
    const String & /* host_id_ */,
    const String & database_name_,
    const String & /* database_zk_path_ */,
    const String & table_name_,
    const String & error_message_)
{
    if (error_message_.empty())
        LOG_TRACE(log, "Created table {}.{}", database_name_, table_name_);
    else
        LOG_TRACE(log, "Failed to created table {}.{}: {}", database_name_, table_name_, error_message_);
}

/// Wait for another host to create a table in a replicated database.
void RestoreCoordinationLocal::waitForCreatingTableInReplicatedDB(
    const String & /* database_name_ */,
    const String & /* database_zk_path_ */,
    const String & /* table_name_ */,
    std::chrono::seconds /* timeout_ */)
{
}

void RestoreCoordinationLocal::finishRestoringMetadata(const String & /* host_id */, const String & error_message_)
{
    LOG_TRACE(log, "Finished restoring metadata{}", (error_message_.empty() ? "" : (" with error " + error_message_)));
}

void RestoreCoordinationLocal::waitForAllHostsToRestoreMetadata(const Strings & /* host_ids_ */, std::chrono::seconds /* timeout_ */) const
{
}

void RestoreCoordinationLocal::setReplicatedTableDataPath(const String & /* host_id_ */,
                                                          const DatabaseAndTableName & table_name_,
                                                          const String & table_zk_path_,
                                                          const String & data_path_in_backup_)
{
    std::lock_guard lock{mutex};
    auto it = replicated_tables_data_paths.find(table_zk_path_);
    if (it == replicated_tables_data_paths.end())
    {
        ReplicatedTableDataPath new_info;
        new_info.table_name = table_name_;
        new_info.data_path_in_backup = data_path_in_backup_;
        replicated_tables_data_paths.emplace(table_zk_path_, std::move(new_info));
        return;
    }
    else
    {
        auto & cur_info = it->second;
        if (table_name_ < cur_info.table_name)
        {
            cur_info.table_name = table_name_;
            cur_info.data_path_in_backup = data_path_in_backup_;
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
    const String & /* host_id_ */, const DatabaseAndTableName & table_name_, const String & table_zk_path_, const String & partition_name_)
{
    std::lock_guard lock{mutex};
    auto key = std::pair{table_zk_path_, partition_name_};
    auto it = replicated_tables_partitions.try_emplace(std::move(key), table_name_).first;
    return it->second == table_name_;
}

}
