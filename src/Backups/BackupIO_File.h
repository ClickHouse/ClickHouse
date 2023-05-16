#pragma once

#include <filesystem>
#include <Backups/BackupIO.h>
#include <Interpreters/Context_fwd.h>

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
    void copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path,
                        WriteMode write_mode, const WriteSettings & write_settings) override;
    DataSourceDescription getDataSourceDescription() const override;

private:
    std::filesystem::path path;
    Poco::Logger * log;
};

class BackupWriterFile : public IBackupWriter
{
public:
    explicit BackupWriterFile(const String & path_, const ContextPtr & context_);
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
