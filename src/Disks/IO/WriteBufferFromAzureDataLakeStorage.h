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
        const AzureBlobStorage::Endpoint & endpoint_,
        const AzureBlobStorage::AuthMethod & auth_method_,
        const Azure::Storage::Blobs::BlobClientOptions & blob_client_options_,
        const String & blob_path_,
        size_t buf_size_,
        const WriteSettings & write_settings_,
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

    LoggerPtr log;

    Azure::Storage::Files::DataLake::DataLakeFileClient file_client;
    const std::string blob_path;
    const WriteSettings write_settings;
    const size_t max_unexpected_write_error_retries;

    bool file_created = false;
    bool is_prefinalized = false;
    int64_t bytes_appended = 0;

    String container_for_logging;
    BlobStorageLogWriterPtr blob_log;
};

Azure::Storage::Files::DataLake::DataLakeFileClient makeAdlsGen2FileClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    const String & blob_path);

bool isAdlsGen2Endpoint(const AzureBlobStorage::Endpoint & endpoint);

}

#endif
