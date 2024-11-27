#pragma once

#include <Core/Types.h>


namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class SeekableReadBuffer;
class WriteBuffer;
enum class WriteMode : uint8_t;
struct WriteSettings;
struct ReadSettings;

/// Represents operations of loading from disk or downloading for reading a backup.
/// See also implementations: BackupReaderFile, BackupReaderDisk.
class IBackupReader
{
public:
    virtual ~IBackupReader() = default;

    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;

    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) = 0;

    /// The function copyFileToDisk() can be much faster than reading the file with readFile() and then writing it to some disk.
    /// (especially for S3 where it can use CopyObject to copy objects inside S3 instead of downloading and uploading them).
    /// Parameters:
    /// `encrypted_in_backup` specify if this file is encrypted in the backup, so it shouldn't be encrypted again while restoring to an encrypted disk.
    virtual void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) = 0;

    virtual const ReadSettings & getReadSettings() const = 0;
    virtual const WriteSettings & getWriteSettings() const = 0;
    virtual size_t getWriteBufferSize() const = 0;
};

/// Represents operations of storing to disk or uploading for writing a backup.
/// See also implementations: BackupWriterFile, BackupWriterDisk
class IBackupWriter
{
public:
    virtual ~IBackupWriter() = default;

    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual bool fileContentsEqual(const String & file_name, const String & expected_file_contents) = 0;

    virtual std::unique_ptr<WriteBuffer> writeFile(const String & file_name) = 0;

    using CreateReadBufferFunction = std::function<std::unique_ptr<SeekableReadBuffer>()>;
    virtual void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) = 0;

    /// The function copyFileFromDisk() can be much faster than copyDataToFile()
    /// (especially for S3 where it can use CopyObject to copy objects inside S3 instead of downloading and uploading them).
    /// Parameters:
    /// `start_pos` and `length` specify a part of the file on `src_disk` to copy to the backup.
    /// `copy_encrypted` specify whether this function should copy encrypted data of the file `src_path` to the backup.
    virtual void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                  bool copy_encrypted, UInt64 start_pos, UInt64 length) = 0;

    virtual void copyFile(const String & destination, const String & source, size_t size) = 0;

    /// Removes a file written to the backup, if it still exists.
    virtual void removeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;

    /// Removes the backup folder if it's empty or contains empty subfolders.
    virtual void removeEmptyDirectories() = 0;

    virtual const ReadSettings & getReadSettings() const = 0;
    virtual const WriteSettings & getWriteSettings() const = 0;
    virtual size_t getWriteBufferSize() const = 0;
};

}
