#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <Common/BlobStorageLogWriter.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <azure/storage/files/datalake/datalake_file_client.hpp>

namespace DB
{

class WriteBufferFromAzureDataLakeStorage : public WriteBufferFromFileBase
{
public:
    WriteBufferFromAzureDataLakeStorage(
        const String & file_url_,
        const AzureBlobStorage::AuthMethod & auth_method_,
        const Azure::Storage::Blobs::BlobClientOptions & blob_client_options_,
        const String & blob_path_,
        size_t buf_size_,
        std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
        const String & container_for_logging_ = {},
        BlobStorageLogWriterPtr blob_log_ = {});

    ~WriteBufferFromAzureDataLakeStorage() override;

    void nextImpl() override;
    void finalizeImpl() override;
    void preFinalize() override;
    std::string getFileName() const override { return blob_path; }
    void sync() override { next(); }

private:
    void ensureCreated();
    void appendBufferedData();
    void runWithRetries(const std::function<void()> & op, const char * what);

    Azure::Storage::Files::DataLake::DataLakeFileClient buildClient(
        const String & file_url,
        const AzureBlobStorage::AuthMethod & auth_method,
        const Azure::Storage::Blobs::BlobClientOptions & blob_client_options);

    LoggerPtr log;

    Azure::Storage::Files::DataLake::DataLakeFileClient file_client;
    const std::string blob_path;
    const size_t max_unexpected_write_error_retries;
    const size_t sdk_retry_initial_backoff_ms;
    const size_t sdk_retry_max_backoff_ms;

    bool file_created = false;
    bool is_prefinalized = false;
    int64_t bytes_appended = 0;

    String container_for_logging;
    BlobStorageLogWriterPtr blob_log;
};

}

#endif
