#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationHelpers.h>
#include <Common/ZooKeeper/Common.h>
#include <map>
#include <unordered_map>


namespace DB
{

/// Stores backup temporary information in Zookeeper, used to perform BACKUP ON CLUSTER.
class BackupCoordinationDistributed : public IBackupCoordination
{
public:
    BackupCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_);
    ~BackupCoordinationDistributed() override;

    void addReplicatedPartNames(
        const String & host_id,
        const StorageID & table_id,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums,
        const String & table_zk_path) override;

    bool hasReplicatedPartNames(const String & host_id, const StorageID & table_id) const override;

    void addReplicatedTableDataPath(const String & host_id, const StorageID & table_id, const String & table_data_path) override;

    void finishPreparing(const String & host_id, const String & error_message) override;
    void waitForAllHostsPrepared(const Strings & host_ids, std::chrono::seconds timeout) const override;

    Strings getReplicatedPartNames(const String & host_id, const StorageID & table_id) const override;
    Strings getReplicatedTableDataPaths(const String & host_id, const StorageID & table_id) const override;

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
    BackupCoordinationDistributedBarrier preparing_barrier;
    mutable std::optional<BackupCoordinationReplicatedPartNames> replicated_part_names;
};

}
