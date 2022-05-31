#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationHelpers.h>


namespace DB
{

/// Stores backup temporary information in Zookeeper, used to perform BACKUP ON CLUSTER.
class BackupCoordinationDistributed : public IBackupCoordination
{
public:
    BackupCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_);
    ~BackupCoordinationDistributed() override;

    void syncStage(const String & current_host, int new_stage, const Strings & wait_hosts, std::chrono::seconds timeout) override;
    void syncStageError(const String & current_host, const String & error_message) override;

    void addReplicatedPartNames(
        const String & table_zk_path,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;

    Strings getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_zk_path, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_zk_path) const override;

    void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() const override;
    Strings listFiles(const String & prefix, const String & terminator) const override;
    std::optional<FileInfo> getFileInfo(const String & file_name) const override;
    std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) const override;
    std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) const override;

    String getNextArchiveSuffix() override;
    Strings getAllArchiveSuffixes() const override;

    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();
    void prepareReplicatedPartNames() const;

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;

    BackupCoordinationStageSync stage_sync;

    mutable std::mutex mutex;
    mutable std::optional<BackupCoordinationReplicatedPartNames> replicated_part_names;
};

}
