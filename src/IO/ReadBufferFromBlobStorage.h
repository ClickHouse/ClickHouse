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
        const String & path_,
        UInt64 max_single_read_retries_,
        size_t buf_size_
    );

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    bool nextImpl() override;

private:

    void initialize();

    std::unique_ptr<Azure::Core::IO::BodyStream> data_stream;
    Azure::Storage::Blobs::BlobContainerClient blob_container_client;
    std::vector<uint8_t> tmp_buffer;
    UInt64 max_single_read_retries; // TODO: unused field
    const String & path;
    off_t offset = 0;
    size_t buf_size;
    size_t total_size;
    bool initialized = false;

};

}

#endif
