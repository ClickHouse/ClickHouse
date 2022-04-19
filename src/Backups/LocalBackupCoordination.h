#include <Backups/IBackupCoordination.h>
#include <map>


namespace DB
{

/// Stores backup contents information in memory.
class LocalBackupCoordination : public IBackupCoordination
{
public:
    LocalBackupCoordination();
    ~LocalBackupCoordination() override;

    void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) override;
    void updateFileInfo(const FileInfo & file_info) override;

    std::vector<FileInfo> getAllFileInfos() override;
    Strings listFiles(const String & prefix, const String & terminator) override;

    std::optional<FileInfo> getFileInfo(const String & file_name) override;
    std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) override;
    std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) override;

    String getNextArchiveSuffix() override;
    Strings getAllArchiveSuffixes() override;

private:
    std::mutex mutex;
    std::map<String /* file_name */, SizeAndChecksum> file_names; /// Should be ordered alphabetically, see listFiles(). For empty files we assume checksum = 0.
    std::map<SizeAndChecksum, FileInfo> file_infos; /// Information about files. Without empty files.
    Strings archive_suffixes;
    size_t current_archive_suffix = 0;
};

}
