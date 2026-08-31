#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>
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

namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
}

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
        /// `memcpy` must not be called with a null source, and an empty response has one.
        if (to_read != 0)
            memcpy(buffer, data.data() + position, to_read);
        position += to_read;
        return to_read;
    }

    std::vector<uint8_t> data;
    int64_t reported_length;
    size_t position = 0;
};

/// The bytes of the blob at positions `start`..`start + size - 1`: every byte of the blob holds
/// the low 8 bits of its position, so that every byte can be attributed to its position.
std::vector<uint8_t> countingBytes(size_t start, size_t size)
{
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>(start + i);
    return data;
}

void assertCountsUpFromZero(const std::string & data)
{
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(i)) << "at position " << i;
}

/// Serves a blob whose every byte holds the low 8 bits of its position, but misbehaves in the
/// two ways a remote endpoint can: every response carries at most `max_response_size` bytes no
/// matter how much was requested (an endpoint that caps or truncates its ranged responses), and
/// no byte at position `served_size` or beyond is ever served (a blob that is shorter than the
/// caller believes), while `blob_size` is what the endpoint claims in `Content-Range`.
class MisbehavingRangeTransport : public Azure::Core::Http::HttpTransport
{
public:
    MisbehavingRangeTransport(
        size_t max_response_size_,
        size_t served_size_,
        size_t blob_size_,
        bool send_etag_,
        std::optional<int64_t> reported_length_ = {})
        : max_response_size(max_response_size_)
        , served_size(served_size_)
        , blob_size(blob_size_)
        , send_etag(send_etag_)
        , reported_length(reported_length_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        /// "x-ms-range: bytes=<start>-<end>", where "-<end>" is optional.
        size_t range_start = 0;
        if (auto range = request.GetHeader("x-ms-range"); range.HasValue())
        {
            const std::string & value = range.Value();
            if (const size_t eq_pos = value.find('='); eq_pos != std::string::npos)
                range_start = std::stoull(value.substr(eq_pos + 1));
        }

        const size_t response_size = range_start < served_size
            ? std::min(max_response_size, served_size - range_start)
            : 0;
        const size_t range_end = range_start + (response_size == 0 ? 0 : response_size - 1);

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");

        /// The length of the body as the endpoint chooses to report it, which is not necessarily
        /// the number of bytes the body actually holds.
        const int64_t length_to_report = reported_length.value_or(static_cast<int64_t>(response_size));

        response->SetHeader("Content-Length", std::to_string(length_to_report));
        response->SetHeader(
            "Content-Range",
            "bytes " + std::to_string(range_start) + "-" + std::to_string(range_end) + "/" + std::to_string(blob_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        if (send_etag)
            response->SetHeader("ETag", "\"0x8DA000000000000\"");
        response->SetBodyStream(std::make_unique<LyingBodyStream>(countingBytes(range_start, response_size), length_to_report));

        return response;
    }

private:
    size_t max_response_size;
    size_t served_size;
    size_t blob_size;
    bool send_etag;
    std::optional<int64_t> reported_length;
};

/// Reads a blob from an endpoint that answers every ranged request with at most
/// `max_response_size` bytes and never serves a byte at position `served_size` or beyond,
/// with the right bound set to `read_until_position` and a `buffer_size`-byte reading buffer.
std::string readWithRightBound(
    size_t max_response_size,
    size_t served_size,
    size_t blob_size,
    size_t read_until_position,
    size_t buffer_size,
    size_t max_read_retries = 1,
    bool send_etag = true)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(max_response_size, served_size, blob_size, send_etag);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1);

    buffer.setReadUntilPosition(read_until_position);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// Reads a whole blob of `blob_size` bytes without any right bound, from an endpoint that answers
/// every request with at most `max_response_size` bytes and reports `reported_length` as the
/// `Content-Length` of every response no matter how many bytes it actually sends.
std::string readWithoutRightBound(size_t max_response_size, size_t blob_size, size_t buffer_size, int64_t reported_length)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport
        = std::make_shared<MisbehavingRangeTransport>(max_response_size, blob_size, blob_size, /* send_etag */ true, reported_length);

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

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// A buffer over an endpoint that serves a blob of `blob_size` bytes, `max_response_size` bytes
/// at a time, without any read having been performed on it yet.
std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeFreshBuffer(size_t max_response_size, size_t blob_size)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, blob_size, blob_size, /* send_etag */ true);

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

/// The endpoint reports a `Content-Length` smaller than the bytes the body can actually produce:
/// the reported length must not truncate the copy, because the requested size is the only bound
/// that matters and the copy stops at the actual end of the body anyway.
TEST(AzureBodyStreamCopy, LengthSmallerThanData)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(8, 0x5A), 1);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
    for (char byte : destination)
        ASSERT_EQ(static_cast<uint8_t>(byte), 0x5A);
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
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 128, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same, with a reading buffer larger than the requested range: a single response must not
/// overrun the right bound either.
TEST(AzureReadUntilPosition, OverlongRangeResponseWithLargeBuffer)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 128, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 1024));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A well-behaved endpoint returns exactly the requested range.
TEST(AzureReadUntilPosition, ExactRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 100, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The endpoint answers every ranged request with at most 40 bytes, less than was requested. The
/// right bound is authoritative: the reader must reopen the download at the new offset and hand
/// the caller all 100 requested bytes instead of reporting the end of the file at 40 bytes.
TEST(AzureReadUntilPosition, ShortRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 40, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
        /* max_read_retries */ 4));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The blob ends at 40 bytes no matter how often the download is reopened, while the caller asked
/// to read until position 100. A bounded read must either reach the right bound or fail - it must
/// not silently report the end of the file before the bound.
TEST(AzureReadUntilPosition, TruncatedBlob)
{
    try
    {
        readWithRightBound(
            /* max_response_size */ 40, /* served_size */ 40, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
            /* max_read_retries */ 3);
        FAIL() << "Expected an exception on a premature end of the response before the right bound";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// `supportsReadAt` promises that a positioned read may be performed on a buffer on which nothing
/// has been read yet, so `readBigAt` must not depend on any state that only the sequential path or
/// `tryGetFileSize` creates.
TEST(AzureReadBigAt, OnFreshBuffer)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100);
    ASSERT_TRUE(buffer->supportsReadAt());

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// The `ETag` response header is optional, and `Azure::ETag::ToString` aborts the process when the
/// tag is absent, so an endpoint that does not send one must not take the server down with it.
TEST(AzureReadUntilPosition, ResponseWithoutETag)
{
    std::string data;
    ASSERT_NO_THROW(
        data = readWithRightBound(
            /* max_response_size */ 100, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
            /* max_read_retries */ 1, /* send_etag */ false));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An unbounded sequential read of an endpoint that under-reports the `Content-Length` of its
/// response: the reported length must not become the end of the file, otherwise a broken endpoint
/// can silently truncate the data. The whole 100-byte body of the response must be delivered even
/// though it claims to be a single byte long.
TEST(AzureReadWithoutRightBound, LengthSmallerThanResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 100, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 1));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An endpoint that reports a length larger than the data it sends must not make the reader hang
/// or report more data than it received: the end of the body is the end of the file.
TEST(AzureReadWithoutRightBound, LengthLargerThanBlob)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 100, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 1000));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

#endif
