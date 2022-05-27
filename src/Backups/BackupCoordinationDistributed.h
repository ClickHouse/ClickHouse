#pragma once

#include <Backups/IBackupCoordination.h>
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

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
};

}
