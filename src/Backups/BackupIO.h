#pragma once

#include <Core/Types.h>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class SeekableReadBuffer;
class WriteBuffer;

/// Represents operations of loading from disk or downloading for reading a backup.
class IBackupReader /// BackupReaderFile, BackupReaderDisk
{
public:
    explicit IBackupReader(Poco::Logger * log_);

    virtual ~IBackupReader() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) = 0;
    virtual void copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path,
                                WriteMode write_mode, const WriteSettings & write_settings);
    virtual DataSourceDescription getDataSourceDescription() const = 0;

protected:
    Poco::Logger * const log;
};

/// Represents operations of storing to disk or uploading for writing a backup.
class IBackupWriter /// BackupWriterFile, BackupWriterDisk
{
public:
    using CreateReadBufferFunction = std::function<std::unique_ptr<SeekableReadBuffer>()>;

    IBackupWriter(const ContextPtr & context_, Poco::Logger * log_);

    virtual ~IBackupWriter() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual bool fileContentsEqual(const String & file_name, const String & expected_file_contents) = 0;

    virtual std::unique_ptr<WriteBuffer> writeFile(const String & file_name) = 0;

    virtual void copyDataToFile(const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name);

    /// copyFileFromDisk() can be much faster than copyDataToFile()
    /// (especially for S3 where it can use CopyObject to copy objects inside S3 instead of downloading and uploading them).
    virtual void copyFileFromDisk(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name);

    virtual void removeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;

    virtual DataSourceDescription getDataSourceDescription() const = 0;

protected:
    Poco::Logger * const log;

    /// These read settings are used to read from the source disk in copyFileFromDisk().
    const ReadSettings read_settings;
};

}
