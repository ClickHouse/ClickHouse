#pragma once

#include <Backups/IBackup.h>
#include <Backups/BackupInfo.h>
#include <map>
#include <mutex>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Represents a backup stored on a disk.
/// A backup is stored as a directory, each entry is stored as a file in that directory.
/// Also three system files are stored:
/// 1) ".base_backup" is a text file with information about the base backup.
/// 2) ".contents" is a binary file containing a list of all entries along with their sizes
/// and checksums and information whether the base backup should be used for each entry
class BackupInDirectory : public IBackup
{
public:
    BackupInDirectory(
        const String & backup_name_,
        OpenMode open_mode_,
        const DiskPtr & disk_,
        const String & path_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info = {});
    ~BackupInDirectory() override;

    const String & getName() const override { return backup_name; }
    OpenMode getOpenMode() const override { return open_mode; }
    time_t getTimestamp() const override { return timestamp; }
    UUID getUUID() const override { return uuid; }
    Strings list(const String & prefix, const String & terminator) const override;
    bool exists(const String & name) const override;
    size_t getSize(const String & name) const override;
    UInt128 getChecksum(const String & name) const override;
    BackupEntryPtr read(const String & name) const override;
    void write(const String & name, BackupEntryPtr entry) override;
    void finalizeWriting() override;

private:
    void open();
    void close();
    void writeMetadata();
    void readMetadata();

    struct EntryInfo
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
    time_t timestamp;
    DiskPtr disk;
    String path;
    ContextPtr context;
    std::optional<BackupInfo> base_backup_info;
    std::shared_ptr<const IBackup> base_backup;
    std::optional<UUID> base_backup_uuid;
    std::map<String, EntryInfo> infos;
    bool directory_was_created = false;
    bool finalized = false;
    mutable std::mutex mutex;
};

}
