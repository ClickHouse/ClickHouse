#pragma once

#include <Core/Types.h>

namespace DB
{
class SeekableReadBuffer;
class WriteBuffer;

/// Represents operations of loading from disk or downloading for reading a backup.
class IBackupReader /// BackupReaderFile, BackupReaderDisk, BackupReaderS3
{
public:
    virtual ~IBackupReader() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual size_t getFileSize(const String & file_name) = 0;
    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) = 0;
};

/// Represents operations of storing to disk or uploading for writing a backup.
class IBackupWriter /// BackupWriterFile, BackupWriterDisk, BackupWriterS3
{
public:
    virtual ~IBackupWriter() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual bool fileContentsEqual(const String & file_name, const String & expected_file_contents) = 0;
    virtual std::unique_ptr<WriteBuffer> writeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;
};

}
