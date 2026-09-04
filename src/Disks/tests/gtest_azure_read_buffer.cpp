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
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
    extern const int FILE_CHANGED_DURING_READ;
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

/// How the endpoint handles the generation of the object across the several requests of one
/// logical read: which `ETag` it reports, whether that tag changes under the reader, and whether
/// it evaluates the `If-Match` precondition the way a real endpoint does.
struct ETagBehaviour
{
    static constexpr const char * first_generation = "\"0x8DA000000000000\"";
    static constexpr const char * second_generation = "\"0x8DA111111111111\"";
    /// The same tag as `first_generation` in the spelling of a blob listing, whose `Etag` element
    /// carries the bare tag, while the `ETag` header of a response carries it quoted.
    static constexpr const char * first_generation_bare = "0x8DA000000000000";

    /// The `ETag` reported by the first response.
    std::string etag = first_generation;
    /// The `ETag` reported from the second response on. Empty means the object never changes.
    std::string etag_after_first;
    /// A real endpoint answers `412 Precondition Failed` when `If-Match` does not hold; one that
    /// ignores the precondition serves the new generation instead.
    bool honour_if_match = false;
};

/// Serves a blob whose every byte holds the low 8 bits of its position, but misbehaves in the
/// two ways a remote endpoint can: every response carries at most `max_response_size` bytes no
/// matter how much was requested (an endpoint that caps or truncates its ranged responses), and
/// no byte at position `served_size` or beyond is ever served (a blob that is shorter than the
/// caller believes), while `blob_size` is what the endpoint claims in `Content-Range`. With
/// `ignore_range`, the endpoint disregards the requested range altogether and answers every
/// request with `200 OK` and the object from byte 0, the way an endpoint that does not support
/// ranged requests does. With `blob_size_after_first`, the total advertised in `Content-Range`
/// changes to that value from the second response on (an endpoint whose idea of the object size
/// is not stable across the requests of one read).
class MisbehavingRangeTransport : public Azure::Core::Http::HttpTransport
{
public:
    MisbehavingRangeTransport(
        size_t max_response_size_,
        size_t served_size_,
        size_t blob_size_,
        bool send_etag_,
        std::optional<int64_t> reported_length_ = {},
        bool ignore_range_ = false,
        ETagBehaviour etags_ = {},
        std::optional<size_t> blob_size_after_first_ = {})
        : max_response_size(max_response_size_)
        , served_size(served_size_)
        , blob_size(blob_size_)
        , send_etag(send_etag_)
        , reported_length(reported_length_)
        , ignore_range(ignore_range_)
        , etags(std::move(etags_))
        , blob_size_after_first(blob_size_after_first_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        const bool first_response = responses_sent == 0;
        const std::string & current_etag = (first_response || etags.etag_after_first.empty())
            ? etags.etag
            : etags.etag_after_first;
        const size_t current_blob_size = first_response ? blob_size : blob_size_after_first.value_or(blob_size);
        ++responses_sent;

        if (etags.honour_if_match)
        {
            if (auto if_match = request.GetHeader("if-match"); if_match.HasValue() && if_match.Value() != current_etag)
            {
                auto failure = std::make_unique<Azure::Core::Http::RawResponse>(
                    1, 1, Azure::Core::Http::HttpStatusCode::PreconditionFailed, "The condition specified using HTTP conditional header(s) is not met.");
                failure->SetHeader("Content-Length", "0");
                failure->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
                return failure;
            }
        }

        /// "x-ms-range: bytes=<start>-<end>", where "-<end>" is optional.
        size_t range_start = 0;
        if (auto range = request.GetHeader("x-ms-range"); range.HasValue() && !ignore_range)
        {
            const std::string & value = range.Value();
            if (const size_t eq_pos = value.find('='); eq_pos != std::string::npos)
                range_start = std::stoull(value.substr(eq_pos + 1));
        }

        const size_t response_size = range_start < served_size
            ? std::min(max_response_size, served_size - range_start)
            : 0;
        const size_t range_end = range_start + (response_size == 0 ? 0 : response_size - 1);

        auto response = ignore_range
            ? std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK")
            : std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");

        /// The length of the body as the endpoint chooses to report it, which is not necessarily
        /// the number of bytes the body actually holds.
        const int64_t length_to_report = reported_length.value_or(static_cast<int64_t>(response_size));

        response->SetHeader("Content-Length", std::to_string(length_to_report));
        /// A `200 OK` response to a ranged request carries no `Content-Range`.
        if (!ignore_range)
            response->SetHeader(
                "Content-Range",
                "bytes " + std::to_string(range_start) + "-" + std::to_string(range_end) + "/" + std::to_string(current_blob_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        if (send_etag)
            response->SetHeader("ETag", current_etag);
        response->SetBodyStream(std::make_unique<LyingBodyStream>(countingBytes(range_start, response_size), length_to_report));

        return response;
    }

private:
    size_t max_response_size;
    size_t served_size;
    size_t blob_size;
    bool send_etag;
    std::optional<int64_t> reported_length;
    bool ignore_range;
    ETagBehaviour etags;
    std::optional<size_t> blob_size_after_first;
    size_t responses_sent = 0;
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
    bool send_etag = true,
    bool ignore_range = false)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, served_size, blob_size, send_etag, /* reported_length */ std::nullopt, ignore_range);

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
/// `Content-Length` of every response no matter how many bytes it actually sends. With
/// `blob_size_after_first`, every response after the first advertises that total in `Content-Range`.
std::string readWithoutRightBound(
    size_t max_response_size,
    size_t blob_size,
    size_t buffer_size,
    int64_t reported_length,
    size_t max_read_retries = 1,
    std::optional<size_t> served_size = {},
    std::optional<size_t> known_object_size = {},
    std::optional<size_t> blob_size_after_first = {})
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, served_size.value_or(blob_size), blob_size, /* send_etag */ true, reported_length,
        /* ignore_range */ false, ETagBehaviour{}, blob_size_after_first);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ {},
        /* container_for_logging */ {},
        known_object_size);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// Reads a whole blob through an endpoint whose object generation behaves as `etags` says, with
/// the read pinned to `expected_etag`.
std::string readPinnedToETag(
    size_t max_response_size,
    size_t blob_size,
    size_t buffer_size,
    const std::string & expected_etag,
    const ETagBehaviour & etags,
    size_t max_read_retries = 4)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, etags);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ {},
        /* container_for_logging */ {},
        blob_size,
        expected_etag);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// A positioned-read buffer pinned to `expected_etag` over an endpoint that reports `etags`.
std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeFreshBufferPinnedToETag(
    size_t blob_size, const std::string & expected_etag, const ETagBehaviour & etags)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        blob_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, etags);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ DB::BlobStorageLogWriterPtr{},
        /* container_for_logging */ std::string{},
        blob_size,
        expected_etag);
}

/// A buffer over an endpoint that serves a blob of `blob_size` bytes, `max_response_size` bytes
/// at a time, without any read having been performed on it yet. With `ignore_range`, the endpoint
/// answers every request with `200 OK` and the object from byte 0.
std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeFreshBuffer(size_t max_response_size, size_t blob_size, bool ignore_range = false)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt, ignore_range);

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

/// An unbounded read of an endpoint that caps every open-ended request to 40 bytes of a 100-byte
/// object. The end of a response body is not the end of the file: the reader must reopen the
/// download at the offset it reached and reassemble the whole object, instead of reporting the end
/// of the file after the first response.
TEST(AzureReadWithoutRightBound, ResponseShorterThanObject)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 40,
        /* max_read_retries */ 4));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same endpoint, except that it never serves a byte at position 40 or beyond while still
/// advertising a 100-byte object. Reopening the download does not help, so the read must fail
/// rather than hand the caller a silently truncated file.
TEST(AzureReadWithoutRightBound, TruncatedObject)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 40);
        FAIL() << "Expected an exception on a response that ends before the advertised size of the object";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// The size of the object is not known locally, and the endpoint's idea of it shrinks between the
/// requests of one read: the first response advertises `bytes 0-39/1000`, the reopen after it
/// advertises `bytes 40-79/80`, and nothing is served from byte 80 on. The lower bound learnt from
/// the first response must stand - the object was declared to be at least 1000 bytes long - so the
/// end of the second response at byte 80 is a premature end of the response and the read must
/// fail rather than silently return 80 bytes as the whole file.
TEST(AzureReadWithoutRightBound, ShrinkingReportedObjectSize)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 1000, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 80, /* known_object_size */ std::nullopt, /* blob_size_after_first */ 80);
        FAIL() << "Expected an exception when a later response advertises a smaller object than an earlier one did";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// `readBigAt` asks for bytes 40..55, but the endpoint ignores the range and answers `200 OK` with
/// the whole object from byte 0. Consuming that body would hand the caller bytes 0..15 under the
/// offsets 40..55, so the read must fail instead.
TEST(AzureReadBigAt, RangeIgnoredByEndpoint)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100, /* ignore_range */ true);

    std::string destination(16, '\0');
    try
    {
        buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 40, {});
        FAIL() << "Expected an exception on a full-object response to a positioned read at a nonzero offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    }
}

/// A `200 OK` response with the whole object is a correct answer to a positioned read that starts
/// at byte 0, so it must be accepted there.
TEST(AzureReadBigAt, FullObjectResponseAtZero)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100, /* ignore_range */ true);

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// A sequential read that starts at byte 40 after a seek, against an endpoint that ignores the
/// range and answers `200 OK` with the object from byte 0: the reader must not advance as though
/// the bytes it received belonged to offset 40.
TEST(AzureSequentialRead, RangeIgnoredAfterSeek)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100, /* ignore_range */ true);
    buffer->seek(40, SEEK_SET);

    try
    {
        buffer->next();
        FAIL() << "Expected an exception on a full-object response to a sequential read at a nonzero offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    }
}

/// The endpoint honours nothing about the range: it answers every request with `200 OK` and at
/// most 40 bytes of the object from byte 0. The first response is a correct answer to the request
/// that started at byte 0, but when the reader reopens the download at offset 40 it gets bytes
/// 0..39 again. Those must not be delivered as bytes 40..79: the read must fail.
TEST(AzureReadUntilPosition, RangeIgnoredOnReopen)
{
    try
    {
        readWithRightBound(
            /* max_response_size */ 40, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
            /* max_read_retries */ 4, /* send_etag */ true, /* ignore_range */ true);
        FAIL() << "Expected an exception on a full-object response to a reopened download at a nonzero offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    }
}

/// The size of the object is known locally, from the `LIST` or `HEAD` that produced the
/// `StoredObject`, while the endpoint caps every open-ended request to 40 bytes and claims in its
/// `Content-Range` that the whole object is 40 bytes long. The locally known size wins: the reader
/// must reopen the download and reassemble all 100 bytes instead of accepting the size that the
/// very response it is validating advertises.
TEST(AzureReadWithoutRightBound, KnownSizeShortFirstResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 40, /* blob_size */ 40, /* buffer_size */ 64, /* reported_length */ 40,
        /* max_read_retries */ 4, /* served_size */ 100, /* known_object_size */ 100));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same, except that the endpoint really has only 40 bytes to serve while the size known
/// locally is 100. Reopening does not help, and the read must fail rather than report a file that
/// is shorter than the caller already knows it to be.
TEST(AzureReadWithoutRightBound, KnownSizeTruncatedObject)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 40, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 40, /* known_object_size */ 100);
        FAIL() << "Expected an exception on a response that ends before the locally known size of the object";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// A read of an object whose generation does not change must not be disturbed by the `If-Match`
/// precondition or by the check of the `ETag` of the response.
TEST(AzureReadPinnedToETag, UnchangedObject)
{
    std::string data;
    ASSERT_NO_THROW(data = readPinnedToETag(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
        ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The blob is overwritten in place after the first response, and the endpoint evaluates
/// `If-Match`, so the reopened download is rejected with `412 Precondition Failed`. That must be
/// reported as the object having changed - and not retried, since it never becomes true again.
TEST(AzureReadPinnedToETag, PreconditionFailedOnReopen)
{
    try
    {
        readPinnedToETag(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
        FAIL() << "Expected an exception on a blob overwritten between two requests of one read";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The same overwrite, against an endpoint that ignores `If-Match` and serves the new generation
/// anyway. The `ETag` of the response must still be compared with the expected one, otherwise the
/// caller receives one logical file stitched together from two generations of the blob.
TEST(AzureReadPinnedToETag, ETagChangedOnReopen)
{
    try
    {
        readPinnedToETag(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = false});
        FAIL() << "Expected an exception on a response that carries a different ETag than the read is pinned to";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The expected tag normally comes from a blob listing, which spells it bare, while the endpoint
/// spells the same tag quoted in the `ETag` header and expects the quoted form in `If-Match`. The
/// two spellings of one tag must be recognized as the same generation: the `If-Match` sent must
/// satisfy an endpoint that evaluates it literally, and the response tag must not be mistaken for
/// a change of the object.
TEST(AzureReadPinnedToETag, BareExpectedETag)
{
    std::string data;
    ASSERT_NO_THROW(data = readPinnedToETag(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
        ETagBehaviour::first_generation_bare, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The difference in spelling must not hide a real change of the object either.
TEST(AzureReadPinnedToETag, BareExpectedETagChangedOnReopen)
{
    try
    {
        readPinnedToETag(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
            ETagBehaviour::first_generation_bare,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = false});
        FAIL() << "Expected an exception on a response that carries a different ETag than the read is pinned to";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// A positioned read is pinned to the generation of the object as well: `readBigAt` is used for
/// column chunks of the same file, and mixing generations between them is just as wrong.
TEST(AzureReadBigAt, ETagChanged)
{
    auto buffer = makeFreshBufferPinnedToETag(
        /* blob_size */ 100, ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = false});

    std::string destination(16, '\0');
    try
    {
        buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {});
        FAIL() << "Expected an exception on a positioned read of a different generation of the blob";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// A positioned read of the generation it is pinned to must succeed.
TEST(AzureReadBigAt, ETagUnchanged)
{
    auto buffer = makeFreshBufferPinnedToETag(
        /* blob_size */ 100, ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// The same for a positioned read pinned to a tag in the bare spelling of a listing.
TEST(AzureReadBigAt, BareExpectedETag)
{
    auto buffer = makeFreshBufferPinnedToETag(
        /* blob_size */ 100, ETagBehaviour::first_generation_bare, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

#endif
