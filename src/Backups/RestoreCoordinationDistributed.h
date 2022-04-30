#pragma once

#include <Backups/IRestoreCoordination.h>
#include <Common/ZooKeeper/Common.h>


namespace DB
{

/// Stores restore temporary information in Zookeeper, used to perform RESTORE ON CLUSTER.
class RestoreCoordinationDistributed : public IRestoreCoordination
{
public:
    RestoreCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_);
    ~RestoreCoordinationDistributed() override;

    /// Sets that a specified host has finished restoring metadata, successfully or with an error.
    /// In the latter case `error_message` should be set.
    void finishRestoringMetadata(const String & host_id_, const String & error_message_) override;

    /// Waits for all hosts to finish restoring their metadata (i.e. to finish creating databases and tables). Returns false if time is out.
    void waitHostsToRestoreMetadata(const Strings & host_ids_, std::chrono::seconds timeout_) const override;

    /// Sets whether a specified table in a replicated database existed before the RESTORE command.
    void setTableExistedInReplicatedDB(const String & host_id_,
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
    String getReplicatedTableDataPath(const String & table_zk_path) const override;

    /// Sets that this replica is going to restore a partition in a replicated table.
    /// The function returns false if this partition is being already restored by another replica.
    bool startRestoringReplicatedTablePartition(
        const String & host_id_,
        const DatabaseAndTableName & table_name_,
        const String & table_zk_path_,
        const String & partition_name_) override;

    /// Removes remotely stored information.
    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
    const Poco::Logger * log;
};

}
