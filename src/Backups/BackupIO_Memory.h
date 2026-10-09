#pragma once

#include <Backups/BackupIO_Default.h>


namespace DB
{
class BackupInMemory;

/// Reads from a backup stored in memory (see class BackupInMemory):
/// RESTORE ... FROM Memory('backup_name')
class BackupReaderMemory : public BackupReaderDefault
{
public:
    BackupReaderMemory(std::shared_ptr<const BackupInMemory> backup_in_memory_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name) override;

private:
    std::shared_ptr<const BackupInMemory> backup_in_memory;
};


/// Writes to a backup stored in memory (see class BackupInMemory):
/// BACKUP ... TO Memory('backup_name')
class BackupWriterMemory : public BackupWriterDefault
{
public:
    BackupWriterMemory(std::shared_ptr<BackupInMemory> backup_in_memory_, const ReadSettings & read_settings_, const WriteSettings & write_settings_);

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;
    void copyFile(const String & destination, const String & source, size_t) override;
    void removeFile(const String & file_name) override;
    void removeEmptyDirectories() override;

private:
    /// This readFile() is used only to implement BackupWriterDefault::fileContentsEqual().
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t) override;

    std::shared_ptr<BackupInMemory> backup_in_memory;
};

}
