#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Backups/BackupIO_Default.h>
#include <Common/Logger.h>
#include <Disks/DiskType.h>
#include <IO/S3Common.h>
#include <IO/S3Settings.h>
#include <Interpreters/Context_fwd.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <IO/S3/S3Capabilities.h>

namespace DB
{

/// Represents a backup stored to AWS S3.
class BackupReaderS3 : public BackupReaderDefault
{
public:
    BackupReaderS3(
        const S3::URI & s3_uri_,
        const String & access_key_id_,
        const String & secret_access_key_,
        bool allow_s3_native_copy,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_,
        bool is_internal_backup);
    ~BackupReaderS3() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;

    void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                        DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) override;

private:
    const S3::URI s3_uri;
    const DataSourceDescription data_source_description;
    S3Settings s3_settings;
    std::shared_ptr<S3::Client> client;

    BlobStorageLogWriterPtr blob_storage_log;
};


class BackupWriterS3 : public BackupWriterDefault
{
public:
    BackupWriterS3(
        const S3::URI & s3_uri_,
        const String & access_key_id_,
        const String & secret_access_key_,
        bool allow_s3_native_copy,
        const String & storage_class_name,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_,
        bool is_internal_backup);
    ~BackupWriterS3() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) override;
    void copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                          bool copy_encrypted, UInt64 start_pos, UInt64 length) override;

    void copyFile(const String & destination, const String & source, size_t size) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;
    void removeEmptyDirectories() override {}

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;

    const S3::URI s3_uri;
    const DataSourceDescription data_source_description;
    S3Settings s3_settings;
    std::shared_ptr<S3::Client> client;
    S3Capabilities s3_capabilities;
    BlobStorageLogWriterPtr blob_storage_log;
};

}

#endif
