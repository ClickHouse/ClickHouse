#pragma once

#include <Core/Types.h>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>

namespace DB
{
class SeekableReadBuffer;
class WriteBuffer;

/// Represents operations of loading from disk or downloading for reading a backup.
class IBackupReader /// BackupReaderFile, BackupReaderDisk
{
public:
    virtual ~IBackupReader() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) = 0;
    virtual DataSourceDescription getDataSourceDescription() const = 0;
};

/// Represents operations of storing to disk or uploading for writing a backup.
class IBackupWriter /// BackupWriterFile, BackupWriterDisk
{
public:
    virtual ~IBackupWriter() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual bool fileContentsEqual(const String & file_name, const String & expected_file_contents) = 0;
    virtual std::unique_ptr<WriteBuffer> writeFile(const String & file_name) = 0;
    virtual void removeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;
    virtual DataSourceDescription getDataSourceDescription() const = 0;
    virtual void copyFileThroughBuffer(std::unique_ptr<SeekableReadBuffer> && source, const String & file_name);

    virtual bool supportNativeCopy(DataSourceDescription /* data_source_description */) const
    {
        return false;
    }

    virtual void copyFileNative(DiskPtr from_disk, const String & file_name_from, const String & file_name_to);
};

}
