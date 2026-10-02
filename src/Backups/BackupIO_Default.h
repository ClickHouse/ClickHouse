#pragma once

#include <Backups/BackupIO.h>
#include <Common/Logger.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

#include <filesystem>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class ReadBuffer;
class SeekableReadBuffer;
class WriteBuffer;
enum class WriteMode : uint8_t;

/// fsync the contents of an already-written local file by re-opening it: a natively copied file
/// (`fs::copy`, `IDisk::copyFile`) leaves no buffer to call `sync` on. Increments `FileSync`.
void fsyncBackupFileContents(const std::filesystem::path & path);

/// fsync a directory so that the entries of the files created inside it are persisted. Unlike
/// `LocalDirectorySyncGuard` this throws on failure, so a backup cannot report success while its
/// durability guarantee silently failed. Increments `DirectorySync`.
void fsyncBackupDirectory(const std::filesystem::path & path);

/// Represents operations of loading from disk or downloading for reading a backup.
class BackupReaderDefault : public IBackupReader
{
public:
    BackupReaderDefault(const ReadSettings & read_settings_, const WriteSettings & write_settings_, LoggerPtr log_);
    ~BackupReaderDefault() override = default;

    /// The function copyFileToDisk() can be much faster than reading the file with readFile() and then writing it to some disk.
    /// (especially for S3 where it can use CopyObject to copy objects inside S3 instead of downloading and uploading them).
    /// Parameters:
    /// `encrypted_in_backup` specify if this file is encrypted in the backup, so it shouldn't be encrypted again while restoring to an encrypted disk.
    void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                        DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) override;

    /// Buffered ranged copy, inherited by every reader that has no native ranged copy. `file_size` is unused:
    /// reading through buffers has no size precondition.
    void copyFileRangeToDisk(const String & path_in_backup, size_t offset, size_t size, size_t file_size,
                             bool encrypted_in_backup, DiskPtr destination_disk, const String & destination_path,
                             WriteMode write_mode) override;

    const ReadSettings & getReadSettings() const override { return read_settings; }
    const WriteSettings & getWriteSettings() const override { return write_settings; }
    size_t getWriteBufferSize() const override { return write_buffer_size; }

protected:
    LoggerPtr const log;
    const ReadSettings read_settings;

    /// The write settings are used to write to the source disk in copyFileToDisk().
    const WriteSettings write_settings;
    const size_t write_buffer_size;
};

/// Represents operations of storing to disk or uploading for writing a backup.
class BackupWriterDefault : public IBackupWriter
{
public:
    BackupWriterDefault(const ReadSettings & read_settings_, const WriteSettings & write_settings_, LoggerPtr log_);
    ~BackupWriterDefault() override = default;

    bool fileContentsEqual(const String & file_name, const String & expected_file_contents, String & actual_file_contents) override;
    void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) override;
    void copyFileFromDisk(
        const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
        override;

    void removeFiles(const Strings & file_names) override;
    void removeEmptyDirectories() override;

    const ReadSettings & getReadSettings() const override { return read_settings; }
    const WriteSettings & getWriteSettings() const override { return write_settings; }
    size_t getWriteBufferSize() const override { return write_buffer_size; }

    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override = 0;

protected:
    LoggerPtr const log;

    /// The read settings are used to read from the source disk in copyFileFromDisk().
    const ReadSettings read_settings;

    const WriteSettings write_settings;
    const size_t write_buffer_size;
};

}
