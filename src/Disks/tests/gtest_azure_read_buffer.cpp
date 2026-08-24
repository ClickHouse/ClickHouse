#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/ReadHelpers.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace
{

/// A body stream that reports a `Content-Length` unrelated to the amount of data it holds,
/// the way a malicious or broken remote endpoint can.
class LyingBodyStream : public Azure::Core::IO::BodyStream
{
public:
    LyingBodyStream(std::vector<uint8_t> data_, int64_t reported_length_)
        : data(std::move(data_)), reported_length(reported_length_)
    {
    }

    int64_t Length() const override { return reported_length; }

    void Rewind() override { position = 0; }

private:
    size_t OnRead(uint8_t * buffer, size_t count, const Azure::Core::Context &) override
    {
        const size_t to_read = std::min(count, data.size() - position);
        memcpy(buffer, data.data() + position, to_read);
        position += to_read;
        return to_read;
    }

    std::vector<uint8_t> data;
    int64_t reported_length;
    size_t position = 0;
};

/// A blob of `size` bytes counting up from zero, so that every byte can be attributed to its
/// position in the blob.
std::vector<uint8_t> countingUpFromZero(size_t size)
{
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>(i);
    return data;
}

void assertCountsUpFromZero(const std::string & data)
{
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(i)) << "at position " << i;
}

/// Answers every `Download` with `response_size` bytes from the beginning of the blob, no matter
/// what range was requested.
class FixedSizeResponseTransport : public Azure::Core::Http::HttpTransport
{
public:
    FixedSizeResponseTransport(size_t response_size_, size_t blob_size_, bool send_etag_)
        : response_size(response_size_), blob_size(blob_size_), send_etag(send_etag_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &, const Azure::Core::Context &) override
    {
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");

        response->SetHeader("Content-Length", std::to_string(response_size));
        response->SetHeader(
            "Content-Range", "bytes 0-" + std::to_string(response_size - 1) + "/" + std::to_string(blob_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        if (send_etag)
            response->SetHeader("ETag", "\"0x8DA000000000000\"");
        response->SetBodyStream(
            std::make_unique<LyingBodyStream>(countingUpFromZero(response_size), static_cast<int64_t>(response_size)));

        return response;
    }

private:
    size_t response_size;
    size_t blob_size;
    bool send_etag;
};

/// Reads a blob from an endpoint that answers every ranged request with `response_size` bytes,
/// with the right bound set to `read_until_position` and a `buffer_size`-byte reading buffer.
std::string readWithRightBound(
    size_t response_size, size_t blob_size, size_t read_until_position, size_t buffer_size, bool send_etag = true)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<FixedSizeResponseTransport>(response_size, blob_size, send_etag);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1);

    buffer.setReadUntilPosition(read_until_position);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

}

/// The endpoint returns more data than was requested: the copy must be capped at the size of
/// the destination buffer instead of overflowing it.
TEST(AzureBodyStreamCopy, OverlongResponse)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(1024, 0xAB), 1024);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
    for (char byte : destination)
        ASSERT_EQ(static_cast<uint8_t>(byte), 0xAB);
}

/// The endpoint reports a large `Content-Length` but sends fewer bytes: only the received bytes
/// are copied and reported.
TEST(AzureBodyStreamCopy, ShortResponse)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(3, 0xCD), 1024);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, 3);
}

/// A stream of unknown length must not be copied beyond the destination buffer either.
TEST(AzureBodyStreamCopy, UnknownLength)
{
    const size_t requested = 4;
    LyingBodyStream body_stream(std::vector<uint8_t>(64, 0xEF), -1);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
}

/// The endpoint answers a 100-byte ranged request with 128 bytes. The reader must stop at the right
/// bound instead of handing the extra 28 bytes to the caller: it reads through a 64-byte buffer, so
/// without a locally derived bound the second `nextImpl` call would already deliver bytes 100..127.
TEST(AzureReadUntilPosition, OverlongRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 128, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same, with a reading buffer larger than the requested range: a single response must not
/// overrun the right bound either.
TEST(AzureReadUntilPosition, OverlongRangeResponseWithLargeBuffer)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 128, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 1024));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A well-behaved endpoint returns exactly the requested range.
TEST(AzureReadUntilPosition, ExactRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 100, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An endpoint that returns less than the requested range must not make the reader report bytes it
/// never received.
TEST(AzureReadUntilPosition, ShortRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 40, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(40));
    assertCountsUpFromZero(data);
}

/// The `ETag` response header is optional, and `Azure::ETag::ToString` aborts the process when the
/// tag is absent, so an endpoint that does not send one must not take the server down with it.
TEST(AzureReadUntilPosition, ResponseWithoutETag)
{
    std::string data;
    ASSERT_NO_THROW(
        data = readWithRightBound(
            /* response_size */ 100, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64, /* send_etag */ false));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

#endif
