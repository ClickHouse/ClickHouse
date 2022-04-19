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

    void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() override;
    Strings listFiles(const String & prefix, const String & terminator) override;
    std::optional<FileInfo> getFileInfo(const String & file_name) override;
    std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) override;
    std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) override;

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
