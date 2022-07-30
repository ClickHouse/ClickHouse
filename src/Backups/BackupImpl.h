#pragma once

#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/BackupInfo.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class IBackupCoordination;
class IBackupReader;
class IBackupWriter;
class SeekableReadBuffer;
class IArchiveReader;
class IArchiveWriter;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Implementation of IBackup.
/// Along with passed files it also stores backup metadata - a single file named ".backup" in XML format
/// which contains a list of all files in the backup with their sizes and checksums and information
/// whether the base backup should be used for each entry.
class BackupImpl : public IBackup
{
public:
    struct ArchiveParams
    {
        String archive_name;
        String password;
        String compression_method;
        int compression_level = 0;
        size_t max_volume_size = 0;
    };

    BackupImpl(
        const String & backup_name_,
        const ArchiveParams & archive_params_,
        const std::optional<BackupInfo> & base_backup_info_,
        std::shared_ptr<IBackupReader> reader_,
        const ContextPtr & context_);

    BackupImpl(
        const String & backup_name_,
        const ArchiveParams & archive_params_,
        const std::optional<BackupInfo> & base_backup_info_,
        std::shared_ptr<IBackupWriter> writer_,
        const ContextPtr & context_,
        bool is_internal_backup_ = false,
        const std::shared_ptr<IBackupCoordination> & coordination_ = {},
        const std::optional<UUID> & backup_uuid_ = {});

    ~BackupImpl() override;

    const String & getName() const override { return backup_name; }
    OpenMode getOpenMode() const override { return open_mode; }
    time_t getTimestamp() const override { return timestamp; }
    UUID getUUID() const override { return *uuid; }
    size_t getNumFiles() const override;
    UInt64 getUncompressedSize() const override;
    UInt64 getCompressedSize() const override;
    Strings listFiles(const String & directory, bool recursive) const override;
    bool hasFiles(const String & directory) const override;
    bool fileExists(const String & file_name) const override;
    bool fileExists(const SizeAndChecksum & size_and_checksum) const override;
    UInt64 getFileSize(const String & file_name) const override;
    UInt128 getFileChecksum(const String & file_name) const override;
    SizeAndChecksum getFileSizeAndChecksum(const String & file_name) const override;
    BackupEntryPtr readFile(const String & file_name) const override;
    BackupEntryPtr readFile(const SizeAndChecksum & size_and_checksum) const override;
    void writeFile(const String & file_name, BackupEntryPtr entry) override;
    void finalizeWriting() override;
    bool supportsWritingInMultipleThreads() const override { return !use_archives; }

private:
    using FileInfo = IBackupCoordination::FileInfo;
    class BackupEntryFromBackupImpl;

    void open(const ContextPtr & context);
    void close();
    void closeArchives();

    /// Writes the file ".backup" containing backup's metadata.
    void writeBackupMetadata();
    void readBackupMetadata();

    /// Checks that a new backup doesn't exist yet.
    void checkBackupDoesntExist() const;

    /// Lock file named ".lock" and containing the UUID of a backup is used to own the place where we're writing the backup.
    /// Thus it will not be allowed to put any other backup to the same place (even if the BACKUP command is executed on a different node).
    void createLockFile();
    bool checkLockFile(bool throw_if_failed) const;
    void removeLockFile();

    void removeAllFilesAfterFailure();

    String getArchiveNameWithSuffix(const String & suffix) const;
    std::shared_ptr<IArchiveReader> getArchiveReader(const String & suffix) const;
    std::shared_ptr<IArchiveWriter> getArchiveWriter(const String & suffix);

    /// Increases `uncompressed_size` by a specific value and `num_files` by 1.
    void increaseUncompressedSize(UInt64 file_size);
    void increaseUncompressedSize(const FileInfo & info);

    /// Calculates and sets `compressed_size`.
    void setCompressedSize();

    const String backup_name;
    const ArchiveParams archive_params;
    const bool use_archives;
    const OpenMode open_mode;
    std::shared_ptr<IBackupWriter> writer;
    std::shared_ptr<IBackupReader> reader;
    const bool is_internal_backup;
    std::shared_ptr<IBackupCoordination> coordination;

    mutable std::mutex mutex;
    std::optional<UUID> uuid;
    time_t timestamp = 0;
    size_t num_files = 0;
    UInt64 uncompressed_size = 0;
    UInt64 compressed_size = 0;
    UInt64 version;
    std::optional<BackupInfo> base_backup_info;
    std::shared_ptr<const IBackup> base_backup;
    std::optional<UUID> base_backup_uuid;
    mutable std::unordered_map<String /* archive_suffix */, std::shared_ptr<IArchiveReader>> archive_readers;
    std::pair<String, std::shared_ptr<IArchiveWriter>> archive_writers[2];
    String current_archive_suffix;
    String lock_file_name;
    size_t num_files_written = 0;
    bool writing_finalized = false;
    const Poco::Logger * log;
};

}
