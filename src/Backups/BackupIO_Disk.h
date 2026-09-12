#pragma once

#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>

#include <filesystem>
#include <mutex>
#include <set>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class BackupReaderDisk : public BackupReaderDefault
{
public:
    BackupReaderDisk(const DiskPtr & disk_, const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);
    ~BackupReaderDisk() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name) override;

    void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                        DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) override;

private:
    const DiskPtr disk;
    const std::filesystem::path root_path;
    const DataSourceDescription data_source_description;
};

class BackupWriterDisk : public BackupWriterDefault
{
public:
    BackupWriterDisk(const DiskPtr & disk_, const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);
    ~BackupWriterDisk() override;

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

    const DiskPtr disk;
    const std::filesystem::path root_path;
    const DataSourceDescription data_source_description;

    /// Whether this disk keeps the backup as plain files in the local filesystem, so that fsyncing
    /// those files and their directories makes the backup durable. See `syncFileToDisk`.
    const bool destination_is_plain_local_files;

    /// Directories that received a file synced via `syncFileToDisk`, collected so they can be
    /// fsynced (deepest-first) in `syncDirectoriesToDisk`. Written from the concurrent backup
    /// write path, hence guarded. Only used for local disks.
    std::mutex dirs_to_sync_mutex;
    std::set<std::filesystem::path> dirs_to_sync TSA_GUARDED_BY(dirs_to_sync_mutex);
};

}
