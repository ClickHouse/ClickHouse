#pragma once

#include <Backups/IRestoreCoordination.h>
#include <Backups/BackupCoordinationStageSync.h>


namespace DB
{

/// Implementation of the IRestoreCoordination interface performing coordination via ZooKeeper. It's necessary for "RESTORE ON CLUSTER".
class RestoreCoordinationRemote : public IRestoreCoordination
{
public:
    RestoreCoordinationRemote(
        zkutil::GetZooKeeper get_zookeeper_,
        const String & root_zookeeper_path_,
        const String & restore_uuid_,
        const Strings & all_hosts_,
        const String & current_host_,
        bool is_internal_);

    ~RestoreCoordinationRemote() override;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    void setStage(const String & new_stage, const String & message) override;
    void setError(const Exception & exception) override;
    Strings waitForStage(const String & stage_to_wait) override;
    Strings waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout) override;

    /// Starts creating a table in a replicated database. Returns false if there is another host which is already creating this table.
    bool acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name) override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool acquireInsertingDataIntoReplicatedTable(const String & table_zk_path) override;

    /// Sets that this replica is going to restore a ReplicatedAccessStorage.
    /// The function returns false if this access storage is being already restored by another replica.
    bool acquireReplicatedAccessStorage(const String & access_storage_zk_path) override;

    /// Sets that this replica is going to restore replicated user-defined functions.
    /// The function returns false if user-defined function at a specified zk path are being already restored by another replica.
    bool acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type) override;

    bool hasConcurrentRestores(const std::atomic<size_t> & num_active_restores) const override;

private:
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void createRootNodes();
    void removeAllNodes();

    class ReplicatedDatabasesMetadataSync;

    const zkutil::GetZooKeeper get_zookeeper;
    const String root_zookeeper_path;
    const String restore_uuid;
    const String zookeeper_path;
    const Strings all_hosts;
    const String current_host;
    const size_t current_host_index;
    const bool is_internal;

    std::optional<BackupCoordinationStageSync> stage_sync;

    mutable std::mutex mutex;
    mutable zkutil::ZooKeeperPtr zookeeper;
};

}
