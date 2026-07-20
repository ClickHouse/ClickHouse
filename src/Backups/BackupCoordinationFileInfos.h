#pragma once

#include <Backups/BackupDataFileNameGeneratorType.h>
#include <Backups/BackupFileInfo.h>

#include <functional>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>


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
    struct Config
    {
        bool plain_backup;
        BackupDataFileNameGeneratorType data_file_name_generator;
        size_t data_file_name_prefix_length;

        /// Experimental object packing (see BackupSettings). When off (the default) the assignment below
        /// is byte-for-byte the historical behavior.
        bool pack_format = false;
        UInt64 pack_size = 0;      /// Target size of one pack object.
        UInt64 pack_min_size = 0;  /// Blobs with physical payload below this get packed; others stay their own object.
    };

    explicit BackupCoordinationFileInfos(const Config & config_)
        : config(config_)
    {
    }

    /// Adds file infos for the specified host.
    void addFileInfos(BackupFileInfos && file_infos, const String & host_id);

    /// Returns file infos for the specified host after preparation.
    BackupFileInfos getFileInfos(const String & host_id) const;

    /// Iterates the file infos of all hosts in place, without copying them into a vector.
    void forEachFileInfoForAllHosts(const std::function<void(const BackupFileInfo &)> & callback) const;

    /// Returns a file info by data file index (see BackupFileInfo::data_file_index).
    BackupFileInfo getFileInfoByDataFileIndex(size_t data_file_index) const;

    /// Returns the number of files after deduplication and excluding empty files.
    size_t getNumFiles() const;

    /// Returns the total size of files after deduplication and excluding empty files.
    size_t getTotalSizeOfFiles() const;

private:
    void prepare() const;

    /// Packed mode only: bin-pack representative blobs with a small physical payload into packs of about
    /// config.pack_size and set BackupFileInfo::pack_id (representatives and their duplicates); large blobs
    /// keep their own object. Must run after dedup/reference resolution.
    void assignPacks() const;

    /// before preparation
    const Config config;

    mutable std::unordered_map<String, BackupFileInfos> file_infos;

    /// after preparation
    mutable bool prepared = false;
    mutable std::vector<BackupFileInfo *> file_infos_for_all_hosts;
    mutable size_t num_files{};
    mutable size_t total_size_of_files{};
};

}
