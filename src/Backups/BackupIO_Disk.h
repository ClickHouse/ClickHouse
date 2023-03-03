#pragma once

#include <filesystem>
#include <Backups/BackupIO.h>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class BackupReaderDisk : public IBackupReader
{
public:
    BackupReaderDisk(const DiskPtr & disk_, const String & path_);
    ~BackupReaderDisk() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;
    DataSourceDescription getDataSourceDescription() const override;

private:
    DiskPtr disk;
    std::filesystem::path path;
};

class BackupWriterDisk : public IBackupWriter
{
public:
    BackupWriterDisk(const DiskPtr & disk_, const String & path_);
    ~BackupWriterDisk() override;

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
    DiskPtr disk;
    std::filesystem::path path;
};

}
