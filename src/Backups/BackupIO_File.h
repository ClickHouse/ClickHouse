#pragma once

#include <Backups/BackupIO_Default.h>
#include <Disks/DiskType.h>
#include <filesystem>


namespace DB
{

class BackupReaderFile : public BackupReaderDefault
{
public:
    explicit BackupReaderFile(const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;

    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;

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

    void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                          bool copy_encrypted, UInt64 start_pos, UInt64 length) override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;

    const std::filesystem::path root_path;
    const DataSourceDescription data_source_description;
};

}
