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

protected:
    bool supportNativeCopy(DataSourceDescription destination_data_source_description, WriteMode mode) const override;
    void copyFileToDiskNative(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path, WriteMode mode) override;
    Poco::Logger * getLogger() const override { return log; }

private:
    std::filesystem::path path;
    Poco::Logger * log;
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
    void copyFileNative(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name) override;

private:
    std::filesystem::path path;
};

}
