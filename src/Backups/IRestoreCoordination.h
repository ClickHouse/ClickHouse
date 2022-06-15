#pragma once

#include <Core/Types.h>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/// Keeps information about files contained in a backup.
class IRestoreCoordination
{
public:
    virtual ~IRestoreCoordination() = default;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    virtual bool startCreatingTableInReplicatedDB(
        const String & host_id, const String & database_name, const String & database_zk_path, const String & table_name)
        = 0;

    /// Sets that either we have been created a table in a replicated database or failed doing that.
    /// In the latter case `error_message` should be set.
    /// Calling this function unblocks other hosts waiting for this table to be created (see waitForCreatingTableInReplicatedDB()).
    virtual void finishCreatingTableInReplicatedDB(
        const String & host_id,
        const String & database_name,
        const String & database_zk_path,
        const String & table_name,
        const String & error_message = {})
        = 0;

    /// Wait for another host to create a table in a replicated database.
    virtual void waitForTableCreatedInReplicatedDB(
        const String & database_name,
        const String & database_zk_path,
        const String & table_name,
        std::chrono::seconds timeout = std::chrono::seconds(-1) /* no timeout */)
        = 0;

    /// Adds a path in backup used by a replicated table.
    /// This function can be called multiple times for the same table with different `host_id`, and in that case
    /// getReplicatedTableDataPath() will choose `data_path_in_backup` with the lexicographycally first `host_id`.
    virtual void addReplicatedTableDataPath(
        const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path, const String & data_path_in_backup)
        = 0;

    /// Sets that a specified host has finished restoring metadata, successfully or with an error.
    /// In the latter case `error_message` should be set.
    virtual void finishRestoringMetadata(const String & host_id, const String & error_message = {}) = 0;

    /// Waits for a specified list of hosts to finish restoring their metadata.
    virtual void waitForAllHostsRestoredMetadata(
        const Strings & host_ids, std::chrono::seconds timeout = std::chrono::seconds(-1) /* no timeout */) const = 0;

    /// Gets path in backup used by a replicated table.
    virtual String getReplicatedTableDataPath(const String & table_zk_path) const = 0;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    virtual bool startInsertingDataToPartitionInReplicatedTable(
        const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path, const String & partition_name)
        = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
