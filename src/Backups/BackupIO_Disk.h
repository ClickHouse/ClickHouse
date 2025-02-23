#pragma once

#include <Backups/BackupIO_Default.h>
#include <Common/Logger.h>
#include <Disks/DiskType.h>

#include <filesystem>


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

    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;

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

    void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                          bool copy_encrypted, UInt64 start_pos, UInt64 length) override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;
    void removeEmptyDirectories() override;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;
    void removeEmptyDirectoriesImpl(const std::filesystem::path & current_dir);

    const DiskPtr disk;
    const std::filesystem::path root_path;
    const DataSourceDescription data_source_description;
};

}
