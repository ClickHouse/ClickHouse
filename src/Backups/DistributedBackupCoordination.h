#include <Backups/IBackupCoordination.h>
#include <Common/ZooKeeper/Common.h>
#include <map>
#include <unordered_map>


namespace DB
{

/// Stores backup contents information in Zookeeper, useful for distributed backups.
class DistributedBackupCoordination : public IBackupCoordination
{
public:
    DistributedBackupCoordination(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_);
    ~DistributedBackupCoordination() override;

    void addFileInfo(const FileInfo & file_info, bool & is_new_checksum) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() override;
    Strings listFiles(const String & prefix, const String & terminator) override;
    std::optional<UInt128> getChecksumByFileName(const String & file_name) override;
    std::optional<FileInfo> getFileInfoByChecksum(const UInt128 & checksum) override;
    std::optional<FileInfo> getFileInfoByFileName(const String & file_name) override;

    String getNextArchiveSuffix() override;
    Strings getAllArchiveSuffixes() override;

    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();

    String zookeeper_path;
    zkutil::GetZooKeeper get_zookeeper;
};

}
