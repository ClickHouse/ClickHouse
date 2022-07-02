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

    void setStatus(const String & current_host, const String & new_status, const String & message) override;
    Strings setStatusAndWait(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts) override;
    Strings setStatusAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, UInt64 timeout_ms) override;

    void addReplicatedPartNames(
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;

    Strings getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_shared_id, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_shared_id) const override;

    void addReplicatedAccessPath(const String & access_zk_path, const String & file_path) override;
    Strings getReplicatedAccessPaths(const String & access_zk_path) const override;

    void setReplicatedAccessHost(const String & access_zk_path, const String & host_id) override;
    String getReplicatedAccessHost(const String & access_zk_path) const override;

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

    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();
    void prepareReplicatedPartNames() const;

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;

    BackupCoordinationStatusSync status_sync;

    mutable std::mutex mutex;
    mutable std::optional<BackupCoordinationReplicatedPartNames> replicated_part_names;
};

}
