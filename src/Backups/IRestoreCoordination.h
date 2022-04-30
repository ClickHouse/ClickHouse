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

    /// Sets that a specified host has finished restoring metadata, successfully or with an error.
    /// In the latter case `error_message` should be set.
    virtual void finishRestoringMetadata(const String & host_id, const String & error_message = {}) = 0;

    /// Waits for all hosts to finish restoring metadata. Returns false if time is out.
    virtual void waitHostsToRestoreMetadata(const Strings & host_ids, std::chrono::seconds timeout = std::chrono::seconds::zero()) const = 0;

    /// Sets whether a specified table in a replicated database existed before the RESTORE command.
    virtual void setTableExistedInReplicatedDB(
        const String & host_id,
        const String & database_name,
        const String & database_zk_path,
        const String & table_name,
        bool table_existed)
        = 0;

    /// Checks that all involved tables in all involved replicated databases didn't exist before the RESTORE command.
    /// This function is used to implement RestoreTableCreationMode::kCreate mode for tables in replicated databases
    /// (we need to check if table didn't exist in this mode).
    virtual void checkTablesNotExistedInReplicatedDBs() const = 0;

    /// Sets path in backup used by a replicated table.
    /// This function can be called multiple times for the same table with different `host_id`, and in that case
    /// getReplicatedTableDataPath() will choose `data_path_in_backup` with the lexicographycally first `host_id`.
    virtual void setReplicatedTableDataPath(
        const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path, const String & data_path_in_backup)
        = 0;

    /// Gets path in backup used by a replicated table.
    virtual String getReplicatedTableDataPath(const String & table_zk_path) const = 0;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    virtual bool startRestoringReplicatedTablePartition(
        const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path, const String & partition_name)
        = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
