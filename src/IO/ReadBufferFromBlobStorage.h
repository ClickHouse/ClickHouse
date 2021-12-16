#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/HTTPCommon.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadSettings.h>

#if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Winconsistent-missing-destructor-override"
#    pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#    pragma clang diagnostic ignored "-Wextra-semi"
#    ifdef HAS_SUGGEST_DESTRUCTOR_OVERRIDE
#        pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#    endif
#    ifdef HAS_RESERVED_IDENTIFIER
#        pragma clang diagnostic ignored "-Wreserved-identifier"
#    endif
#endif

#include <azure/storage/blobs.hpp>

#if defined(__clang__)
#    pragma clang diagnostic pop
#endif

namespace DB
{

class ReadBufferFromBlobStorage : public SeekableReadBuffer
{
public:

    explicit ReadBufferFromBlobStorage(
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

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromBlobStorage");
};

}

#endif
