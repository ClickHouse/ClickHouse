#pragma once

#include <filesystem>
#include <Backups/BackupIO.h>
#include <Interpreters/Context_fwd.h>

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
    void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                        DiskPtr destination_disk, const String & destination_path, WriteMode write_mode, const WriteSettings & write_settings) override;

private:
    const DiskPtr disk;
    const std::filesystem::path path;
};

class BackupWriterDisk : public IBackupWriter
{
public:
    BackupWriterDisk(const DiskPtr & disk_, const String & path_, const ContextPtr & context_);
    ~BackupWriterDisk() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    bool fileContentsEqual(const String & file_name, const String & expected_file_contents) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                          bool copy_encrypted, UInt64 start_pos, UInt64 length) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;

private:
    const DiskPtr disk;
    const std::filesystem::path path;
    const bool has_throttling;
};

}
