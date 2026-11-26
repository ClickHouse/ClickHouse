#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <Core/Defines.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/ReadHelpers.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
}

namespace
{

class OwningBodyStream : public Azure::Core::IO::BodyStream
{
public:
    explicit OwningBodyStream(std::vector<uint8_t> data_) : data(std::move(data_)) { }

    int64_t Length() const override { return static_cast<int64_t>(data.size()); }

    void Rewind() override { position = 0; }

private:
    size_t OnRead(uint8_t * buffer, size_t count, const Azure::Core::Context &) override
    {
        const size_t to_read = std::min(count, data.size() - position);
        if (to_read != 0)
            memcpy(buffer, data.data() + position, to_read);
        position += to_read;
        return to_read;
    }

    std::vector<uint8_t> data;
    size_t position = 0;
};

/// A fake Azure endpoint that serves a 10-byte blob in a response without the optional `ETag` header.
class EndpointWithoutETag : public Azure::Core::Http::HttpTransport
{
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &, const Azure::Core::Context &) override
    {
        constexpr size_t blob_size = 10;
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");
        response->SetHeader("Content-Length", std::to_string(blob_size));
        response->SetHeader("Content-Range", "bytes 0-" + std::to_string(blob_size - 1) + "/" + std::to_string(blob_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetHeader("x-ms-blob-type", "BlockBlob");
        response->SetBodyStream(std::make_unique<OwningBodyStream>(std::vector<uint8_t>(blob_size, 'a')));
        return response;
    }
};

}

/// `Azure::ETag::ToString` aborts the process on an absent tag, also in release builds.
/// A download response without `ETag` must fail the read with an exception instead.
TEST(AzureReadBuffer, MissingETagIsAnException)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<EndpointWithoutETag>();

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1);

    std::optional<int> error_code;
    try
    {
        std::string data;
        DB::readStringUntilEOF(data, buffer);
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }
    ASSERT_EQ(error_code, std::optional<int>(DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR));
}

#endif
