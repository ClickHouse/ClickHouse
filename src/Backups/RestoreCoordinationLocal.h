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

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool startCreatingTableInReplicatedDB(
        const String & host_id, const String & database_name, const String & database_zk_path, const String & table_name) override;

    /// Sets that either we have been created a table in a replicated database or failed doing that.
    /// In the latter case `error_message` should be set.
    /// Calling this function unblocks other hosts waiting for this table to be created (see waitForCreatingTableInReplicatedDB()).
    void finishCreatingTableInReplicatedDB(
        const String & host_id,
        const String & database_name,
        const String & database_zk_path,
        const String & table_name,
        const String & error_message) override;

    /// Wait for another host to create a table in a replicated database.
    void waitForTableCreatedInReplicatedDB(
        const String & database_name, const String & database_zk_path, const String & table_name, std::chrono::seconds timeout) override;

    /// Sets path in backup used by a replicated table.
    /// This function can be called multiple times for the same table with different `host_id`, and in that case
    /// getReplicatedTableDataPath() will choose `data_path_in_backup` with the lexicographycally first `host_id`.
    void addReplicatedTableDataPath(
        const String & host_id,
        const DatabaseAndTableName & table_name,
        const String & table_zk_path,
        const String & data_path_in_backup) override;

    /// Sets that a specified host has finished restoring metadata, successfully or with an error.
    /// In the latter case `error_message` should be set.
    void finishRestoringMetadata(const String & host_id, const String & error_message) override;

    /// Waits for all hosts to finish restoring their metadata (i.e. to finish creating databases and tables). Returns false if time is out.
    void waitForAllHostsRestoredMetadata(const Strings & host_ids, std::chrono::seconds timeout) const override;

    /// Gets path in backup used by a replicated table.
    String getReplicatedTableDataPath(const String & table_zk_path) const override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool startInsertingDataToPartitionInReplicatedTable(
        const String & host_id,
        const DatabaseAndTableName & table_name,
        const String & table_zk_path,
        const String & partition_name) override;

private:
    struct ReplicatedTableDataPath
    {
        DatabaseAndTableName table_name;
        String data_path_in_backup;
    };

    std::unordered_map<String /* table_zk_path */, ReplicatedTableDataPath> replicated_tables_data_paths;

    std::map<std::pair<String /* table_zk_path */, String /* partition_name */>, DatabaseAndTableName> replicated_tables_partitions;

    mutable std::mutex mutex;
    const Poco::Logger * log;
};

}
