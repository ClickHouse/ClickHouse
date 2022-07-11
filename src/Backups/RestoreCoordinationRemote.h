#pragma once

#include <Backups/IRestoreCoordination.h>
#include <Backups/BackupCoordinationStatusSync.h>


namespace DB
{

/// Implementation of the IRestoreCoordination interface performing coordination via ZooKeeper. It's necessary for "RESTORE ON CLUSTER".
class RestoreCoordinationRemote : public IRestoreCoordination
{
public:
    RestoreCoordinationRemote(const String & zookeeper_path, zkutil::GetZooKeeper get_zookeeper);
    ~RestoreCoordinationRemote() override;

    /// Sets the current status and waits for other hosts to come to this status too. If status starts with "error:" it'll stop waiting on all the hosts.
    void setStatus(const String & current_host, const String & new_status, const String & message) override;
    void setErrorStatus(const String & current_host, const Exception & exception) override;
    Strings waitStatus(const Strings & all_hosts, const String & status_to_wait) override;
    Strings waitStatusFor(const Strings & all_hosts, const String & status_to_wait, UInt64 timeout_ms) override;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) override;

    /// Sets that this replica is going to restore a ReplicatedAccessStorage.
    /// The function returns false if this access storage is being already restored by another replica.
    bool acquireReplicatedAccessStorage(const String & access_storage_zk_path) override;

    /// Removes remotely stored information.
    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();

    class ReplicatedDatabasesMetadataSync;

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
    BackupCoordinationStatusSync status_sync;
};

}
