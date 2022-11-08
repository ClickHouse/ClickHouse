#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Backups/BackupIO.h>
#include <IO/S3Common.h>
#include <IO/ReadSettings.h>
#include <Storages/StorageS3Settings.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>

namespace DB
{

/// Represents a backup stored to AWS S3.
class BackupReaderS3 : public IBackupReader
{
public:
    BackupReaderS3(const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, const ContextPtr & context_);
    ~BackupReaderS3() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;
    DataSourceDescription getDataSourceDescription() const override;

private:
    S3::URI s3_uri;
    std::shared_ptr<Aws::S3::S3Client> client;
    ReadSettings read_settings;
    S3Settings::RequestSettings request_settings;
};


class BackupWriterS3 : public IBackupWriter
{
public:
    BackupWriterS3(const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, const ContextPtr & context_);
    ~BackupWriterS3() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    bool fileContentsEqual(const String & file_name, const String & expected_file_contents) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;

    DataSourceDescription getDataSourceDescription() const override;
    bool supportNativeCopy(DataSourceDescription data_source_description) const override;
    void copyFileNative(DiskPtr from_disk, const String & file_name_from, const String & file_name_to) override;

private:
    Aws::S3::Model::HeadObjectOutcome requestObjectHeadData(const std::string & bucket_from, const std::string & key) const;

    void copyObjectImpl(
        const String & src_bucket,
        const String & src_key,
        const String & dst_bucket,
        const String & dst_key,
        const Aws::S3::Model::HeadObjectResult & head,
        const std::optional<ObjectAttributes> & metadata = std::nullopt) const;

    void copyObjectMultipartImpl(
        const String & src_bucket,
        const String & src_key,
        const String & dst_bucket,
        const String & dst_key,
        const Aws::S3::Model::HeadObjectResult & head,
        const std::optional<ObjectAttributes> & metadata = std::nullopt) const;

    S3::URI s3_uri;
    std::shared_ptr<Aws::S3::S3Client> client;
    ReadSettings read_settings;
    S3Settings::RequestSettings request_settings;
    Poco::Logger * log;
};

}

#endif
