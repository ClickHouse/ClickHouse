#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationReplicatedTables.h>
#include <Backups/BackupCoordinationStageSync.h>


namespace DB
{

/// Implementation of the IBackupCoordination interface performing coordination via ZooKeeper. It's necessary for "BACKUP ON CLUSTER".
class BackupCoordinationRemote : public IBackupCoordination
{
public:
    BackupCoordinationRemote(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, bool remove_zk_nodes_in_destructor_);
    ~BackupCoordinationRemote() override;

    void setStage(const String & current_host, const String & new_stage, const String & message) override;
    void setError(const String & current_host, const Exception & exception) override;
    Strings waitForStage(const Strings & all_hosts, const String & stage_to_wait) override;
    Strings waitForStage(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout) override;

    void addReplicatedPartNames(
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;

    Strings getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedMutations(
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<MutationInfo> & mutations) override;

    std::vector<MutationInfo> getReplicatedMutations(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_shared_id, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_shared_id) const override;

    void addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path) override;
    Strings getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const override;

    void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() const override;
    Strings listFiles(const String & directory, bool recursive) const override;
    bool hasFiles(const String & directory) const override;
    std::optional<FileInfo> getFileInfo(const String & file_name) const override;
    std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) const override;
    std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) const override;

    String getNextArchiveSuffix() override;
    Strings getAllArchiveSuffixes() const override;

private:
    zkutil::ZooKeeperPtr getZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeperNoLock() const;
    void createRootNodes();
    void removeAllNodes();
    void prepareReplicatedTables() const;
    void prepareReplicatedAccess() const;

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
    const bool remove_zk_nodes_in_destructor;

    std::optional<BackupCoordinationStageSync> stage_sync;

    mutable std::mutex mutex;
    mutable zkutil::ZooKeeperPtr zookeeper;
    mutable std::optional<BackupCoordinationReplicatedTables> replicated_tables;
    mutable std::optional<BackupCoordinationReplicatedAccess> replicated_access;
};

}
