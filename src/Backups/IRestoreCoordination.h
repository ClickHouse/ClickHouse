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

    /// Sets the current stage and waits for other hosts to come to this stage too.
    virtual void syncStage(const String & current_host, int stage, const Strings & wait_hosts, std::chrono::seconds timeout) = 0;

    /// Sets that the current host encountered an error, so other hosts should know that and stop waiting in syncStage().
    virtual void syncStageError(const String & current_host, const String & error_message) = 0;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    virtual bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) = 0;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    virtual bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) = 0;

    /// Sets that this replica is going to restore a ReplicatedAccessStorage.
    /// The function returns false if this access storage is being already restored by another replica.
    virtual bool acquireReplicatedAccessStorage(const String & access_storage_zk_path) = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
