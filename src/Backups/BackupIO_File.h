#pragma once

#include <filesystem>
#include <Backups/BackupIO.h>

namespace DB
{

class BackupReaderFile : public IBackupReader
{
public:
    BackupReaderFile(const String & path_);
    ~BackupReaderFile() override;

    bool fileExists(const String & file_name) override;
    size_t getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;

private:
    std::filesystem::path path;
};

class BackupWriterFile : public IBackupWriter
{
public:
    BackupWriterFile(const String & path_);
    ~BackupWriterFile() override;

    bool fileExists(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;
    void removeFilesAfterFailure(const Strings & file_names) override;

private:
    std::filesystem::path path;
};

}
