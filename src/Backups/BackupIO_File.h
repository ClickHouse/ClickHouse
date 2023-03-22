#pragma once

#include <filesystem>
#include <Backups/BackupIO.h>

namespace DB
{

class BackupReaderFile : public IBackupReader
{
public:
    explicit BackupReaderFile(const String & path_);
    ~BackupReaderFile() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;
    DataSourceDescription getDataSourceDescription() const override;

private:
    std::filesystem::path path;
};

class BackupWriterFile : public IBackupWriter
{
public:
    explicit BackupWriterFile(const String & path_);
    ~BackupWriterFile() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    bool fileContentsEqual(const String & file_name, const String & expected_file_contents) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;
    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;
    DataSourceDescription getDataSourceDescription() const override;
    bool supportNativeCopy(DataSourceDescription data_source_description) const override;

    void copyFileNative(DiskPtr from_disk, const String & file_name_from, const String & file_name_to) override;

private:
    std::filesystem::path path;
};

}
