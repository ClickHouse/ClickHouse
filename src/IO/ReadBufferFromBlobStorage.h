#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/SeekableReadBuffer.h>
#include <azure/storage/blobs.hpp>

namespace DB
{

class ReadBufferFromBlobStorage : public SeekableReadBuffer
{
public:
    explicit ReadBufferFromBlobStorage(
        Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
        const String & main_path_,
        const String & path_,
        size_t buf_size_
    );

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    bool nextImpl() override;

private:
    std::unique_ptr<ReadBuffer> impl;
    off_t offset = 0;

    Azure::Storage::Blobs::BlobContainerClient blob_container_client;
    const String & main_path;
    const String & path;
    size_t buf_size;

    std::unique_ptr<ReadBuffer> initialize();

};

}

#endif
