#pragma once

#include <Poco/Util/AbstractConfiguration.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>

#include <Disks/IDisk.h>

#include <Backups/BackupInfo.h>
#include <Backups/IBackup.h>


namespace DB
{

/** DiskBackup adapts backup interface to disk interface and additionally performs paths translation.
  *
  * It is used internally by DatabaseBackup. For MergeTree tables DatabaseBackup creates special storage policy
  * that uses DiskBackup. Additional path translation layer is required because database relative path inside the backup
  * will be completely different from the DatabaseBackup relative path.
  *
  * For example when the MergeTree table inside the database Backup tries to read some file, it uses the relative path of the
  * database Backup that gets translated inside the DiskBackup to a proper path that can be used to query files/directories in backup.
  */
class DiskBackup final : public IDisk
{
public:
    struct PathPrefixReplacement
    {
        std::string from;
        std::string to;
    };

    DiskBackup(std::shared_ptr<IBackup> backup_,
        PathPrefixReplacement path_prefix_replacement_,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix);

    const String & getPath() const override { return root_path; }

    ReservationPtr reserve(UInt64 bytes) override;

    std::optional<UInt64> getTotalSpace() const override;
    std::optional<UInt64> getAvailableSpace() const override;
    std::optional<UInt64> getUnreservedSpace() const override;

    bool existsFile(const String & path) const override;
    bool existsDirectory(const String & path) const override;
    bool existsFileOrDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override;

    DirectoryIteratorPtr iterateDirectory(const String & path) const override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copyDirectoryContent(
        const String & from_dir,
        const std::shared_ptr<IDisk> & to_disk,
        const String & to_dir,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override;

    void listFiles(const String & path, std::vector<String> & file_names) const override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) override;

    Strings getBlobPath(const String & path) const override;
    bool areBlobPathsRandom() const override { return false; }
    void writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) const override;

    time_t getLastChanged(const String & path) const override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void truncateFile(const String & path, size_t size) override;

    DataSourceDescription getDataSourceDescription() const override;

    bool isRemote() const override { return false; }

    bool supportZeroCopyReplication() const override { return false; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &) override;

    bool isBroken() const override { return false; }

    bool isReadOnly() const override { return true; }

    void shutdown() override;

private:
    const std::shared_ptr<IBackup> backup;
    const PathPrefixReplacement path_prefix_replacement;
    const LoggerPtr logger;
    const std::string root_path;

    String replacePathPrefix(const String & path) const;

    mutable std::mutex mutex;
    std::unordered_map<std::string, Poco::Timestamp> last_modified;
};

}
