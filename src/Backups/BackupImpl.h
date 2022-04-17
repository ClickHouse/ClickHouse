#pragma once

#include <Backups/IBackup.h>
#include <Backups/BackupInfo.h>
#include <map>
#include <mutex>
#include <unordered_map>


namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class SeekableReadBuffer;

/// Base implementation of IBackup.
/// Along with passed files it also stores backup metadata - a single file named ".backup" in XML format
/// which contains a list of all files in the backup with their sizes and checksums and information
/// whether the base backup should be used for each entry.
class BackupImpl : public IBackup
{
public:
    BackupImpl(
        const String & backup_name_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});
    ~BackupImpl() override;

    const String & getName() const override { return backup_name; }
    void open(OpenMode open_mode_) override;
    OpenMode getOpenMode() const override;
    void close() override;
    time_t getTimestamp() const override;
    UUID getUUID() const override { return uuid; }
    Strings listFiles(const String & prefix, const String & terminator) const override;
    bool fileExists(const String & file_name) const override;
    bool fileExistsByChecksum(const UInt128 & checksum) const override;
    size_t getFileSize(const String & file_name) const override;
    size_t getFileSizeByChecksum(const UInt128 & checksum) const override;
    UInt128 getFileChecksum(const String & file_name) const override;
    BackupEntryPtr readFile(const String & file_name) const override;
    BackupEntryPtr readFileByChecksum(const UInt128 & checksum) const override;
    void writeFile(const String & file_name, BackupEntryPtr entry) override;
    void finalizeWriting() override;

protected:
    /// Checks if this backup exists.
    virtual bool backupExists() const = 0;

    virtual void openImpl(OpenMode open_mode_) = 0;
    OpenMode getOpenModeNoLock() const { return open_mode; }

    virtual void closeImpl(const Strings & written_files_, bool writing_finalized_) = 0;

    /// Read a file from the backup.
    /// Low level: the function doesn't check base backup or checksums.
    virtual std::unique_ptr<SeekableReadBuffer> readFileImpl(const String & file_name) const = 0;

    /// Add a file to the backup.
    /// Low level: the function doesn't check base backup or checksums.
    virtual std::unique_ptr<WriteBuffer> writeFileImpl(const String & file_name) = 0;

    mutable std::mutex mutex;

private:
    void writeBackupMetadata();
    void readBackupMetadata();

    struct FileInfo
    {
        String file_name;

        UInt64 size = 0;
        UInt128 checksum{0, 0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0, 0};
    };

    class IFileInfos
    {
    public:
        virtual ~IFileInfos() {}
        virtual void add(FileInfo && file_info, bool & is_new_checksum) = 0;
        void add(FileInfo && file_info) { bool dummy; add(std::move(file_info), dummy); }
        virtual std::vector<FileInfo> getAllFileInfos() = 0;
        virtual Strings listFiles(const String & prefix, const String & terminator) = 0;
        virtual std::optional<UInt128> getChecksumByFileName(const String & file_name) = 0;
        virtual std::optional<FileInfo> getFileInfoByChecksum(const UInt128 & checksum) = 0;
        virtual std::optional<FileInfo> getFileInfoByFileName(const String & file_name) = 0;
    };

    class BackupEntryFromBackupImpl;
    class LocalFileInfos;

    const String backup_name;
    ContextPtr context;
    const std::optional<BackupInfo> base_backup_info_param;
    OpenMode open_mode = OpenMode::NONE;
    UUID uuid = {};
    time_t timestamp = 0;
    UInt64 version = 1;
    std::optional<BackupInfo> base_backup_info;
    std::shared_ptr<const IBackup> base_backup;
    std::optional<UUID> base_backup_uuid;
    std::unique_ptr<IFileInfos> file_infos;
    bool writing_finalized = false;
};

}
