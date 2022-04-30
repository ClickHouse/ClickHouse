#pragma once

#include <Backups/IRestoreCoordination.h>
#include <condition_variable>
#include <map>
#include <mutex>
#include <unordered_map>

namespace Poco { class Logger; }


namespace DB
{

class RestoreCoordinationLocal : public IRestoreCoordination
{
public:
    RestoreCoordinationLocal();
    ~RestoreCoordinationLocal() override;

    /// Sets that a specified host has finished restoring metadata, successfully or with an error.
    /// In the latter case `error_message` should be set.
    void finishRestoringMetadata(const String & host_id_, const String & error_message_) override;

    /// Waits for all hosts to finish restoring their metadata (i.e. to finish creating databases and tables). Returns false if time is out.
    void waitHostsToRestoreMetadata(const Strings & host_ids_, std::chrono::seconds timeout_) const override;

    /// Sets whether a specified table in a replicated database existed before the RESTORE command.
    void setTableExistedInReplicatedDB(
        const String & host_id_,
        const String & database_name_,
        const String & database_zk_path_,
        const String & table_name_,
        bool table_existed_) override;

    /// Checks that all involved tables in all involved replicated databases didn't exist before the RESTORE command.
    /// This function is used to implement RestoreTableCreationMode::kCreate mode for tables in replicated databases
    /// (we need to check if table didn't exist in this mode).
    void checkTablesNotExistedInReplicatedDBs() const override;

    /// Sets path in backup used by a replicated table.
    /// This function can be called multiple times for the same table with different `host_id`, and in that case
    /// getReplicatedTableDataPath() will choose `data_path_in_backup` with the lexicographycally first `host_id`.
    void setReplicatedTableDataPath(
        const String & host_id_,
        const DatabaseAndTableName & table_name_,
        const String & table_zk_path_,
        const String & data_path_in_backup_) override;

    /// Gets path in backup used by a replicated table.
    String getReplicatedTableDataPath(const String & table_zk_path_) const override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool startRestoringReplicatedTablePartition(
        const String & host_id_,
        const DatabaseAndTableName & table_name_,
        const String & table_zk_path_,
        const String & partition_name_) override;

private:
    struct TableExistedStatus
    {
        DatabaseAndTableName table_name;
        bool table_existed = false;
    };

    std::map<std::pair<String /* database_zk_path */, String /* table_name */>, TableExistedStatus> table_existed_in_replicated_db;

    struct ReplicatedTableDataPath
    {
        DatabaseAndTableName table_name;
        String data_path_in_backup;
    };

    std::unordered_map<String /* table_zk_path */, ReplicatedTableDataPath> replicated_table_data_paths;

    std::map<std::pair<String /* table_zk_path */, String /* partition_name */>, DatabaseAndTableName> replicated_table_partitions;

    mutable std::mutex mutex;
    const Poco::Logger * log;
};

}
