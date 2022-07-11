#pragma once

#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <IO/HTTPCommon.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadSettings.h>
#include <azure/storage/blobs.hpp>

namespace DB
{

class ReadBufferFromAzureBlobStorage : public SeekableReadBuffer
{
public:

    explicit ReadBufferFromAzureBlobStorage(
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        const String & path_,
        size_t max_single_read_retries_,
        size_t max_single_download_retries_,
        size_t tmp_buffer_size_,
        bool use_external_buffer_ = false,
        size_t read_until_position_ = 0
    );

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    bool nextImpl() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

private:

    void initialize();

    std::unique_ptr<Azure::Core::IO::BodyStream> data_stream;
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    std::unique_ptr<Azure::Storage::Blobs::BlobClient> blob_client;

    const String path;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
    std::vector<char> tmp_buffer;
    size_t tmp_buffer_size;
    bool use_external_buffer;
    off_t read_until_position = 0;

    off_t offset = 0;
    size_t total_size;
    bool initialized = false;
    char * data_ptr;
    size_t data_capacity;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromAzureBlobStorage");
};

}

#endif
