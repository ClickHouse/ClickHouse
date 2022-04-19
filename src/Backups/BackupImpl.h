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
        std::shared_ptr<IBackupWriter> writer_,
        std::shared_ptr<IBackupCoordination> coordination_,
        bool is_helper_backup_,
        const ContextPtr & context_);

    BackupImpl(
        const String & backup_name_,
        const ArchiveParams & archive_params_,
        const std::optional<BackupInfo> & base_backup_info_,
        std::shared_ptr<IBackupReader> reader_,
        const ContextPtr & context_);

    ~BackupImpl() override;

    const String & getName() const override { return backup_name; }
    OpenMode getOpenMode() const override { return open_mode; }
    time_t getTimestamp() const override;
    UUID getUUID() const override { return uuid; }
    Strings listFiles(const String & prefix, const String & terminator) const override;
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

    void open();
    void close();
    void writeBackupMetadata();
    void readBackupMetadata();
    String getArchiveNameWithSuffix(const String & suffix) const;
    std::shared_ptr<IArchiveReader> getArchiveReader(const String & suffix) const;
    std::shared_ptr<IArchiveWriter> getArchiveWriter(const String & suffix);
    void removeAllFilesAfterFailure();

    const String backup_name;
    const ArchiveParams archive_params;
    const bool use_archives;
    const std::optional<BackupInfo> base_backup_info_initial;
    const OpenMode open_mode;
    std::shared_ptr<IBackupWriter> writer;
    std::shared_ptr<IBackupReader> reader;
    std::shared_ptr<IBackupCoordination> coordination;
    const bool is_helper_backup;
    ContextPtr context;

    mutable std::mutex mutex;
    UUID uuid = {};
    time_t timestamp = 0;
    UInt64 version;
    std::optional<BackupInfo> base_backup_info;
    std::shared_ptr<const IBackup> base_backup;
    std::optional<UUID> base_backup_uuid;
    mutable std::unordered_map<String /* archive_suffix */, std::shared_ptr<IArchiveReader>> archive_readers;
    std::shared_ptr<IArchiveWriter> archive_writer_with_empty_suffix;
    std::shared_ptr<IArchiveWriter> current_archive_writer;
    String current_archive_suffix;
    bool writing_finalized = false;
};

}
