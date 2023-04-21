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
    virtual ~IBackupReader() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) = 0;
    virtual void copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path,
                                WriteMode write_mode, const WriteSettings & write_settings);
    virtual DataSourceDescription getDataSourceDescription() const = 0;
};

/// Represents operations of storing to disk or uploading for writing a backup.
class IBackupWriter /// BackupWriterFile, BackupWriterDisk
{
public:
    using CreateReadBufferFunction = std::function<std::unique_ptr<SeekableReadBuffer>()>;

    explicit IBackupWriter(const ContextPtr & context_);

    virtual ~IBackupWriter() = default;
    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual bool fileContentsEqual(const String & file_name, const String & expected_file_contents) = 0;
    virtual std::unique_ptr<WriteBuffer> writeFile(const String & file_name) = 0;
    virtual void removeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;
    virtual DataSourceDescription getDataSourceDescription() const = 0;
    virtual void copyDataToFile(const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name);
    virtual bool supportNativeCopy(DataSourceDescription /* data_source_description */) const { return false; }

    /// Copy file using native copy (optimized for S3 to use CopyObject)
    ///
    /// NOTE: It still may fall back to copyDataToFile() if native copy is not possible:
    /// - different buckets
    /// - throttling had been requested
    virtual void copyFileNative(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name);

protected:
    const ReadSettings read_settings;
    const bool has_throttling;
};

}
