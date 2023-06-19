#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <azure/storage/blobs.hpp>
#include <azure/core/io/body_stream.hpp>


namespace Poco
{
class Logger;
}

namespace DB
{

class WriteBufferFromAzureBlobStorage : public WriteBufferFromFileBase
{
public:
    using AzureClientPtr = std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient>;

    WriteBufferFromAzureBlobStorage(
        AzureClientPtr blob_container_client_,
        const String & blob_path_,
        size_t max_single_part_upload_size_,
        size_t buf_size_,
        const WriteSettings & write_settings_);

    ~WriteBufferFromAzureBlobStorage() override;

    void nextImpl() override;

    std::string getFileName() const override { return blob_path; }
    void sync() override { next(); }

private:
    void finalizeImpl() override;
    void execWithRetry(std::function<void()> func, size_t num_tries, size_t cost = 0);
    void uploadBlock(const char * data, size_t size);

    Poco::Logger * log;

    const size_t max_single_part_upload_size;
    const std::string blob_path;
    const WriteSettings write_settings;

    AzureClientPtr blob_container_client;
    std::vector<std::string> block_ids;

    using MemoryBufferPtr = std::unique_ptr<Memory<>>;
    MemoryBufferPtr tmp_buffer;
    size_t tmp_buffer_write_offset = 0;

    MemoryBufferPtr allocateBuffer() const;
};

}

#endif
