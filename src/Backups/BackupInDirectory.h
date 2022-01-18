#pragma once

#include <Backups/IBackup.h>
#include <map>
#include <mutex>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a backup stored on a disk.
/// A backup is stored as a directory, each entry is stored as a file in that directory.
/// Also three system files are stored:
/// 1) ".base" is an XML file with information about the base backup.
/// 2) ".contents" is a binary file containing a list of all entries along with their sizes
/// and checksums and information whether the base backup should be used for each entry
/// 3) ".write_lock" is a temporary empty file which is created before writing of a backup
/// and deleted after finishing that writing.
class BackupInDirectory : public IBackup
{
public:
    BackupInDirectory(OpenMode open_mode_, const DiskPtr & disk_, const String & path_, const std::shared_ptr<const IBackup> & base_backup_ = {});
    ~BackupInDirectory() override;

    OpenMode getOpenMode() const override;
    String getPath() const override;
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
    void writePathToBaseBackup();
    void readPathToBaseBackup();
    void writeContents();
    void readContents();

    struct EntryInfo
    {
        UInt64 size = 0;
        UInt128 checksum{0, 0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0, 0};
    };

    const OpenMode open_mode;
    const DiskPtr disk;
    String path;
    String path_with_sep;
    std::shared_ptr<const IBackup> base_backup;
    std::map<String, EntryInfo> infos;
    bool directory_was_created = false;
    bool finalized = false;
    mutable std::mutex mutex;
};

}
