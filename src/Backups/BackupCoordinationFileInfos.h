#pragma once

#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <Backups/BackupFileInfo.h>


namespace DB
{

/// Hosts use this class to coordinate lists of files they are going to write to a backup.
/// Because different hosts shouldn't write the same file twice and or even files with different names but with the same checksum.
/// Also the initiator of the BACKUP query uses this class to get a whole list of files written by all hosts to write that list
/// as a part of the contents of the .backup file (the backup metadata file).
class BackupCoordinationFileInfos
{
public:
    /// plain_backup sets that we're writing a plain backup, which means all duplicates are written as is, and empty files are written as is.
    /// (For normal backups only the first file amongst duplicates is actually stored, and empty files are not stored).
    explicit BackupCoordinationFileInfos(bool plain_backup_) : plain_backup(plain_backup_) {}

    /// Adds file infos for the specified host.
    void addFileInfos(BackupFileInfos && file_infos, const String & host_id);

    /// Returns file infos for the specified host after preparation.
    BackupFileInfos getFileInfos(const String & host_id) const;

    /// Returns file infos for all hosts after preparation.
    BackupFileInfos getFileInfosForAllHosts() const;

    /// Returns a file info by data file index (see BackupFileInfo::data_file_index).
    BackupFileInfo getFileInfoByDataFileIndex(size_t data_file_index) const;

    /// Returns the number of files after deduplication and excluding empty files.
    size_t getNumFiles() const;

    /// Returns the total size of files after deduplication and excluding empty files.
    size_t getTotalSizeOfFiles() const;

private:
    void prepare() const;

    /// before preparation
    const bool plain_backup;
    mutable std::unordered_map<String, BackupFileInfos> file_infos;

    /// after preparation
    mutable bool prepared = false;
    mutable std::vector<BackupFileInfo *> file_infos_for_all_hosts;
    mutable size_t num_files;
    mutable size_t total_size_of_files;
};

}
