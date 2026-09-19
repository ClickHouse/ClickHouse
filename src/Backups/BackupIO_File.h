#pragma once

#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>

#include <filesystem>
#include <mutex>
#include <set>
#include <vector>


namespace DB
{

class BackupReaderFile : public BackupReaderDefault
{
public:
    explicit BackupReaderFile(const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name) override;

    void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                        DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) override;

private:
    const std::filesystem::path root_path;
    const DataSourceDescription data_source_description;
};

class BackupWriterFile : public BackupWriterDefault
{
public:
    BackupWriterFile(
        const String & root_path_,
        const String & allowed_path_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_);

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyFileFromDisk(
        const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
        override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeEmptyDirectories() override;

    void syncFileToDisk(const String & file_name) override;
    void syncDirectoriesToDisk() override;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;
    void removeEmptyDirectoriesImpl(const std::filesystem::path & current_dir);

    const std::filesystem::path root_path;
    const DataSourceDescription data_source_description;

    /// Directories that received a file synced via `syncFileToDisk`, collected so they can be
    /// fsynced (deepest-first) in `syncDirectoriesToDisk`. Seeded by the constructor with the
    /// `backups.allowed_path` entry containing this backup and the ancestors of it that the backup
    /// has to create; those bound the walk in `syncFileToDisk`, so it stays inside the configured
    /// backup area. Written from the concurrent backup write path, hence guarded.
    std::mutex dirs_to_sync_mutex;
    std::set<std::filesystem::path> dirs_to_sync TSA_GUARDED_BY(dirs_to_sync_mutex);

    /// The ancestors of the backup area, deepest-first, fsynced last so that the area's own entry and
    /// every entry above it is durable. Outside the configured area, hence best-effort: a failure at
    /// one level is logged, not thrown, and does not skip the levels above it. Set by the constructor
    /// and read only by `syncDirectoriesToDisk`, so it needs no locking.
    std::vector<std::filesystem::path> best_effort_dirs_to_sync;
};

}
