#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <azure/storage/blobs.hpp>


namespace DB
{

class WriteBufferFromBlobStorage : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromBlobStorage(
        Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
        const String & blob_path_,
        size_t buf_size_);

    void nextImpl() override;

private:
    Azure::Storage::Blobs::BlobContainerClient blob_container_client;
    const String blob_path;
    size_t buf_size;
};

}

#endif
