#pragma once

#include <Backups/IRestoreCoordination.h>
#include <Backups/BackupCoordinationHelpers.h>


namespace DB
{

/// Stores restore temporary information in Zookeeper, used to perform RESTORE ON CLUSTER.
class RestoreCoordinationDistributed : public IRestoreCoordination
{
public:
    RestoreCoordinationDistributed(const String & zookeeper_path, zkutil::GetZooKeeper get_zookeeper);
    ~RestoreCoordinationDistributed() override;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    void syncStage(const String & current_host, int new_stage, const Strings & wait_hosts, std::chrono::seconds timeout) override;

    /// Sets that the current host encountered an error, so other hosts should know that and stop waiting in syncStage().
    void syncStageError(const String & current_host, const String & error_message) override;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) override;

    /// Removes remotely stored information.
    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();

    class ReplicatedDatabasesMetadataSync;

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
    BackupCoordinationStageSync stage_sync;
};

}
