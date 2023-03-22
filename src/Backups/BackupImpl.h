#pragma once

#include <Backups/IBackup.h>
#include <Backups/BackupInfo.h>
#include <map>
#include <mutex>


namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Base implementation of IBackup.
/// Along with passed files it also stores backup metadata - a single file named ".backup" in XML format
/// which contains a list of all files in the backup with their sizes and checksums and information
/// whether the base backup should be used for each entry.
class BackupImpl : public IBackup
{
public:
    BackupImpl(
        const String & backup_name_,
        OpenMode open_mode_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});
    ~BackupImpl() override;

    const String & getName() const override { return backup_name; }
    OpenMode getOpenMode() const override { return open_mode; }
    time_t getTimestamp() const override { return timestamp; }
    UUID getUUID() const override { return uuid; }
    Strings listFiles(const String & prefix, const String & terminator) const override;
    bool fileExists(const String & file_name) const override;
    size_t getFileSize(const String & file_name) const override;
    UInt128 getFileChecksum(const String & file_name) const override;
    BackupEntryPtr readFile(const String & file_name) const override;
    void addFile(const String & file_name, BackupEntryPtr entry) override;
    void finalizeWriting() override;

protected:
    /// Should be called in the constructor of a derived class.
    void open();

    /// Should be called in the destructor of a derived class.
    void close();

    /// Read a file from the backup.
    /// Low level: the function doesn't check base backup or checksums.
    virtual std::unique_ptr<ReadBuffer> readFileImpl(const String & file_name) const = 0;

    /// Add a file to the backup.
    /// Low level: the function doesn't check base backup or checksums.
    virtual std::unique_ptr<WriteBuffer> addFileImpl(const String & file_name) = 0;

    /// Checks if this backup exists.
    virtual bool backupExists() const = 0;

    /// Starts writing of this backup, only used if `open_mode == OpenMode::WRITE`.
    /// After calling this function `backupExists()` should return true.
    virtual void startWriting() = 0;

    /// Removes all the backup files, called if something goes wrong while we're writing the backup.
    /// This function is called by `close()` if `startWriting()` was called and `finalizeWriting()` wasn't.
    virtual void removeAllFilesAfterFailure() = 0;

private:
    void writeBackupMetadata();
    void readBackupMetadata();

    struct FileInfo
    {
        UInt64 size = 0;
        UInt128 checksum{0, 0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0, 0};
    };

    const String backup_name;
    const OpenMode open_mode;
    UUID uuid;
    time_t timestamp = 0;
    ContextPtr context;
    std::optional<BackupInfo> base_backup_info;
    std::shared_ptr<const IBackup> base_backup;
    std::optional<UUID> base_backup_uuid;
    std::map<String, FileInfo> file_infos;
    bool writing_started = false;
    bool writing_finalized = false;
    mutable std::mutex mutex;
};

}
