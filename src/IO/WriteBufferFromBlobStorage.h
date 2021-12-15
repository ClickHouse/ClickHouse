#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <memory>

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

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
#include <azure/core/io/body_stream.hpp>

#if defined(__clang__)
#    pragma clang diagnostic pop
#endif

namespace DB
{

class WriteBufferFromBlobStorage : public BufferWithOwnMemory<WriteBuffer>
{
public:

    explicit WriteBufferFromBlobStorage(
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        const String & blob_path_,
        size_t max_single_part_upload_size_,
        size_t buf_size_);

    ~WriteBufferFromBlobStorage() override;

    void nextImpl() override;

private:

    void finalizeImpl() override;

    std::vector<std::string> block_ids;
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    size_t max_single_part_upload_size;
    const String blob_path;
};

}

#endif
