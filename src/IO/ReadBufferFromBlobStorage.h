#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/HTTPCommon.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadSettings.h>
#include <azure/storage/blobs.hpp>

namespace DB
{

class ReadBufferFromBlobStorage : public SeekableReadBuffer
{
public:

    explicit ReadBufferFromBlobStorage(
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        const String & path_,
        size_t tmp_buffer_size_,
        bool use_external_buffer_ = false,
        size_t read_until_position_ = 0
    );

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    bool nextImpl() override;

private:

    void initialize();

    std::unique_ptr<Azure::Core::IO::BodyStream> data_stream;
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    std::unique_ptr<Azure::Storage::Blobs::BlobClient> blob_client;
    std::vector<char> tmp_buffer;
    const String path;
    bool use_external_buffer;
    off_t read_until_position = 0;
    off_t offset = 0;
    size_t tmp_buffer_size;
    size_t total_size;
    bool initialized = false;
    char * data_ptr;
    size_t data_capacity;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromBlobStorage");
};

}

#endif
