#pragma once

#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>

#include <filesystem>
#include <mutex>
#include <set>


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
    BackupWriterFile(const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);

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

    /// The deepest directory at or above root_path's parent that already existed when this writer
    /// was constructed. Every directory below it is created by this backup, so its entry is not
    /// durable until the containing directory is fsynced; this one is the last that has to be
    /// fsynced, and the parent walk in syncFileToDisk() stops at (and includes) it. Computed once
    /// before anything is written, and it bounds the walk so it never runs up to "/".
    const std::filesystem::path sync_dirs_up_to;

    /// Directories that received a file synced via syncFileToDisk(), collected so they can be
    /// fsynced (deepest-first) in syncDirectoriesToDisk(). Written from the concurrent backup
    /// write path, hence guarded.
    std::mutex dirs_to_sync_mutex;
    std::set<std::filesystem::path> dirs_to_sync TSA_GUARDED_BY(dirs_to_sync_mutex);
};

}
