#pragma once

#include <Backups/BackupIO_Default.h>


namespace DB
{

/// Writes a backup to /dev/null:
/// BACKUP ... TO Null
class BackupWriterNull : public BackupWriterDefault
{
public:
    BackupWriterNull(const ReadSettings & read_settings_, const WriteSettings & write_settings_);

    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) override;
    void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length) override;
    void copyFile(const String & destination, const String & source, size_t) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;
    void removeEmptyDirectories() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    bool fileContentsEqual(const String & file_name, const String & expected_file_contents, String & actual_file_contents) override;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;
};

}
