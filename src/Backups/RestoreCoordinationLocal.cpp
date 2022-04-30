#include <Backups/RestoreCoordinationLocal.h>
#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/Exception.h>
#include <base/chrono_io.h>
#include <base/logger_useful.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int FAILED_TO_RESTORE_METADATA_ON_OTHER_NODE;
    extern const int LOGICAL_ERROR;
}


RestoreCoordinationLocal::RestoreCoordinationLocal()
    : log(&Poco::Logger::get("RestoreCoordinationLocal"))
{}

RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

void RestoreCoordinationLocal::finishRestoringMetadata(const String & /* host_id */, const String & error_message_)
{
    LOG_TRACE(log, "Finished restoring metadata{}", (error_message_.empty() ? "" : (" with error " + error_message_)));
}

void RestoreCoordinationLocal::waitHostsToRestoreMetadata(const Strings & /* host_ids_ */, std::chrono::seconds /* timeout_ */) const
{
}

void RestoreCoordinationLocal::setTableExistedInReplicatedDB(
    const String & /* host_id_ */,
    const String & database_name_,
    const String & database_zk_path_,
    const String & table_name_,
    bool table_existed_)
{
    std::lock_guard lock{mutex};

    auto key = std::pair{database_zk_path_, table_name_};

    auto it = table_existed_in_replicated_db.find(key);
    if (it == table_existed_in_replicated_db.end())
    {
        TableExistedStatus status;
        status.table_name.first = database_name_;
        status.table_name.second = table_name_;
        status.table_existed = table_existed_;
        table_existed_in_replicated_db.emplace(std::move(key), std::move(status));
        return;
    }
    else
    {
        auto & status = it->second;
        if (!table_existed_)
            status.table_existed = false;
    }
}

void RestoreCoordinationLocal::checkTablesNotExistedInReplicatedDBs() const
{
    std::lock_guard lock{mutex};
    for (const auto & status : table_existed_in_replicated_db | boost::adaptors::map_values)
    {
        if (status.table_existed)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "{} already exists", formatTableNameOrTemporaryTableName(status.table_name));
    }
}

void RestoreCoordinationLocal::setReplicatedTableDataPath(const String & /* host_id_ */,
                                                          const DatabaseAndTableName & table_name_,
                                                          const String & table_zk_path_,
                                                          const String & data_path_in_backup_)
{
    std::lock_guard lock{mutex};
    auto it = replicated_table_data_paths.find(table_zk_path_);
    if (it == replicated_table_data_paths.end())
    {
        ReplicatedTableDataPath new_info;
        new_info.table_name = table_name_;
        new_info.data_path_in_backup = data_path_in_backup_;
        replicated_table_data_paths.emplace(table_zk_path_, std::move(new_info));
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
    auto it = replicated_table_data_paths.find(table_zk_path);
    if (it == replicated_table_data_paths.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replicated data path is not set for zk_path={}", table_zk_path);
    return it->second.data_path_in_backup;
}

bool RestoreCoordinationLocal::startRestoringReplicatedTablePartition(const String & /* host_id_ */,
                                                                      const DatabaseAndTableName & table_name_,
                                                                      const String & table_zk_path_,
                                                                      const String & partition_name_)
{
    std::lock_guard lock{mutex};
    auto key = std::pair{table_zk_path_, partition_name_};
    auto it = replicated_table_partitions.try_emplace(std::move(key), table_name_).first;
    return it->second == table_name_;
}

}
