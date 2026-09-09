#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace
{

class FixedBodyStream : public Azure::Core::IO::BodyStream
{
public:
    FixedBodyStream(std::vector<uint8_t> data_, int64_t reported_length_)
        : data(std::move(data_)), reported_length(reported_length_)
    {
    }

    int64_t Length() const override { return reported_length; }

    void Rewind() override { position = 0; }

private:
    size_t OnRead(uint8_t * buffer, size_t count, const Azure::Core::Context &) override
    {
        const size_t available = data.size() - position;
        const size_t to_read = std::min(count, available);
        if (to_read != 0)
            memcpy(buffer, data.data() + position, to_read);
        position += to_read;
        return to_read;
    }

    std::vector<uint8_t> data;
    int64_t reported_length;
    size_t position = 0;
};

class OverlongResponseTransport : public Azure::Core::Http::HttpTransport
{
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
        Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        constexpr size_t response_size = 32;

        const bool is_download = request.GetMethod() == Azure::Core::Http::HttpMethod::Get;

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1,
            1,
            is_download ? Azure::Core::Http::HttpStatusCode::PartialContent : Azure::Core::Http::HttpStatusCode::Ok,
            is_download ? "Partial Content" : "OK");
        response->SetHeader("Content-Length", std::to_string(response_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetHeader("ETag", "\"0x8DA000000000000\"");
        response->SetHeader("x-ms-blob-type", "BlockBlob");

        if (!is_download)
        {
            response->SetBodyStream(std::make_unique<FixedBodyStream>(std::vector<uint8_t>{}, 0));
            return response;
        }

        response->SetHeader("Content-Range", "bytes 0-31/32");

        std::vector<uint8_t> data(response_size);
        for (size_t i = 0; i < response_size; ++i)
            data[i] = static_cast<uint8_t>(i);
        response->SetBodyStream(std::make_unique<FixedBodyStream>(std::move(data), response_size));
        return response;
    }
};

std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeBuffer()
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<OverlongResponseTransport>();

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1);
}

}

TEST(AzureReadBigAt, DoesNotTrustResponseLength)
{
    constexpr size_t requested = 16;

    auto buffer = makeBuffer();

    buffer->getFileSize(); /// readBigAt needs the blob client, created lazily by the sizing call

    struct Storage
    {
        std::array<char, requested> payload{};
        std::array<char, requested> canary{};
    } storage;

    std::fill(storage.canary.begin(), storage.canary.end(), '\xCD');

    const size_t bytes_read = buffer->readBigAt(storage.payload.data(), requested, /* range_begin */ 0, {});

    ASSERT_EQ(bytes_read, requested);
    for (size_t i = 0; i < requested; ++i)
        ASSERT_EQ(static_cast<uint8_t>(storage.payload[i]), static_cast<uint8_t>(i));
    for (char byte : storage.canary)
        ASSERT_EQ(byte, '\xCD');
}

#endif
