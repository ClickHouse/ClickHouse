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

/// Serves a range response that advertises `claimed_size` bytes but whose body stream only
/// yields `served_size` bytes. claimed_size > requested exercises the overlong branch (the
/// body must not be copied past the caller's buffer); served_size < requested exercises the
/// truncated branch (the body stream hits EOF before the requested amount).
class RangeResponseTransport : public Azure::Core::Http::HttpTransport
{
public:
    RangeResponseTransport(size_t claimed_size_, size_t served_size_)
        : claimed_size(claimed_size_), served_size(served_size_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
        Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        const bool is_download = request.GetMethod() == Azure::Core::Http::HttpMethod::Get;

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1,
            1,
            is_download ? Azure::Core::Http::HttpStatusCode::PartialContent : Azure::Core::Http::HttpStatusCode::Ok,
            is_download ? "Partial Content" : "OK");
        response->SetHeader("Content-Length", std::to_string(claimed_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetHeader("ETag", "\"0x8DA000000000000\"");
        response->SetHeader("x-ms-blob-type", "BlockBlob");

        if (!is_download)
        {
            response->SetBodyStream(std::make_unique<FixedBodyStream>(std::vector<uint8_t>{}, 0));
            return response;
        }

        response->SetHeader("Content-Range", "bytes 0-" + std::to_string(claimed_size - 1) + "/" + std::to_string(claimed_size));

        std::vector<uint8_t> data(served_size);
        for (size_t i = 0; i < served_size; ++i)
            data[i] = static_cast<uint8_t>(i);
        /// Report the claimed (advertised) length while only serving served_size bytes, so a
        /// reader that trusted the reported length would over-read or over-report.
        response->SetBodyStream(std::make_unique<FixedBodyStream>(std::move(data), static_cast<int64_t>(claimed_size)));
        return response;
    }

private:
    size_t claimed_size;
    size_t served_size;
};

std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeBuffer(size_t claimed_size, size_t served_size)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<RangeResponseTransport>(claimed_size, served_size);

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

    /// The response advertises and serves 32 bytes while only 16 are requested.
    auto buffer = makeBuffer(/* claimed_size */ 32, /* served_size */ 32);

    /// The documented handshake: supportsReadAt is the required setup call before readBigAt.
    ASSERT_TRUE(buffer->supportsReadAt());

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

TEST(AzureReadBigAt, ReturnsAccumulatedCountOnTruncatedResponse)
{
    constexpr size_t requested = 16;
    constexpr size_t served = 8;

    /// The response advertises `requested` bytes but its body ends after `served`.
    auto buffer = makeBuffer(/* claimed_size */ requested, /* served_size */ served);

    ASSERT_TRUE(buffer->supportsReadAt());

    struct Storage
    {
        std::array<char, requested> payload{};
        std::array<char, requested> canary{};
    } storage;

    std::fill(storage.payload.begin(), storage.payload.end(), '\xCD');
    std::fill(storage.canary.begin(), storage.canary.end(), '\xCD');

    /// max_single_download_retries == 1, so the short read is not retried: readBigAt must return
    /// the accumulated byte count, not the requested size.
    const size_t bytes_read = buffer->readBigAt(storage.payload.data(), requested, /* range_begin */ 0, {});

    ASSERT_EQ(bytes_read, served);
    for (size_t i = 0; i < served; ++i)
        ASSERT_EQ(static_cast<uint8_t>(storage.payload[i]), static_cast<uint8_t>(i));
    /// The unread tail must be left untouched instead of silently trusted as read.
    for (size_t i = served; i < requested; ++i)
        ASSERT_EQ(storage.payload[i], '\xCD');
    for (char byte : storage.canary)
        ASSERT_EQ(byte, '\xCD');
}

#endif
