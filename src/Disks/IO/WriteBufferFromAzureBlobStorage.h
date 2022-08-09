#pragma once

#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <memory>

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <azure/storage/blobs.hpp>
#include <azure/core/io/body_stream.hpp>


namespace DB
{

class WriteBufferFromAzureBlobStorage : public BufferWithOwnMemory<WriteBuffer>
{
public:

    WriteBufferFromAzureBlobStorage(
        std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        const String & blob_path_,
        size_t max_single_part_upload_size_,
        size_t buf_size_,
        const WriteSettings & write_settings_,
        std::optional<std::map<std::string, std::string>> attributes_ = {});

    void nextImpl() override;

private:
    void finalizeImpl() override;

    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    size_t max_single_part_upload_size;
    const String blob_path;
    WriteSettings write_settings;
    std::optional<std::map<std::string, std::string>> attributes;
};

}

#endif
