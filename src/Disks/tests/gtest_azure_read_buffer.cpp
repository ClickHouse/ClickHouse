#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int AZURE_OBJECT_CHANGED_DURING_READ;
}

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

namespace
{

/// Reads a blob sequentially from an endpoint that answers every ranged request with `response_size`
/// bytes counting up from zero, with the right bound set to `read_until_position` and a
/// `buffer_size`-byte reading buffer.
std::string readWithRightBound(size_t response_size, size_t read_until_position, size_t buffer_size)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<RangeResponseTransport>(response_size, response_size);

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

void assertCountsUpFromZero(const std::string & data)
{
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(i)) << "at position " << i;
}

}

/// The endpoint answers a 100-byte ranged request with 128 bytes. The reader must stop at the right
/// bound instead of handing the extra 28 bytes to the caller: it reads through a 64-byte buffer, so
/// with the bound derived from the `Content-Length` of the response the second `nextImpl` call
/// already delivers bytes 100..127, and only the third one trips the right-bound check.
TEST(AzureReadUntilPosition, OverlongRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 128, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same, with a reading buffer larger than the requested range: a single response must not
/// overrun the right bound either.
TEST(AzureReadUntilPosition, OverlongRangeResponseWithLargeBuffer)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 128, /* read_until_position */ 100, /* buffer_size */ 1024));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A well-behaved endpoint returns exactly the requested range.
TEST(AzureReadUntilPosition, ExactRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 100, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An endpoint that returns less than the requested range must not make the reader report bytes it
/// never received.
TEST(AzureReadUntilPosition, ShortRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(/* response_size */ 40, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(40));
    assertCountsUpFromZero(data);
}

namespace
{

/// Serves every ranged request with the bytes of a blob that counts up from zero, and optionally
/// with `extra_bytes` more than were requested, so that a reader that trusts the length of the
/// response hands out bytes from outside the requested range.
class CountingRangeTransport : public Azure::Core::Http::HttpTransport
{
public:
    static constexpr auto default_etag = "\"0x8DA000000000000\"";

    /// The blob is served with `etag_` as its current generation. An honest endpoint rejects a
    /// download whose `If-Match` names another generation with `412 Precondition Failed`; one with
    /// `honours_if_match_ == false` ignores the condition and serves the current generation anyway.
    explicit CountingRangeTransport(size_t extra_bytes_, std::string etag_ = default_etag, bool honours_if_match_ = true)
        : extra_bytes(extra_bytes_), etag(std::move(etag_)), honours_if_match(honours_if_match_)
    {
    }

    /// How many blob downloads the reader has issued, so that a test can assert that a bound
    /// that allows no bytes at all does not go to the endpoint in the first place.
    size_t getDownloadCount() const { return downloads; }

    /// How many `GetProperties` (`HEAD`) requests the reader has issued.
    size_t getPropertiesCount() const { return properties_requests; }

    /// The `If-Match` condition of the last download, or empty if it had none.
    const std::string & getLastIfMatch() const { return last_if_match; }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
        Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        const bool is_download = request.GetMethod() == Azure::Core::Http::HttpMethod::Get;
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Head)
            ++properties_requests;
        if (is_download)
        {
            ++downloads;

            /// The SDK stores the names of the headers of a request in lower case.
            const auto headers = request.GetHeaders();
            auto it = headers.find("if-match");
            last_if_match = it == headers.end() ? "" : it->second;

            if (honours_if_match && !last_if_match.empty() && last_if_match != etag)
            {
                auto rejection = std::make_unique<Azure::Core::Http::RawResponse>(
                    1, 1, Azure::Core::Http::HttpStatusCode::PreconditionFailed, "Precondition Failed");
                rejection->SetHeader("x-ms-error-code", "ConditionNotMet");
                rejection->SetHeader("Content-Length", "0");
                rejection->SetBodyStream(std::make_unique<FixedBodyStream>(std::vector<uint8_t>{}, 0));
                return rejection;
            }
        }

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1,
            1,
            is_download ? Azure::Core::Http::HttpStatusCode::PartialContent : Azure::Core::Http::HttpStatusCode::Ok,
            is_download ? "Partial Content" : "OK");
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetHeader("ETag", etag);
        response->SetHeader("x-ms-blob-type", "BlockBlob");

        if (!is_download)
        {
            response->SetHeader("Content-Length", "0");
            response->SetBodyStream(std::make_unique<FixedBodyStream>(std::vector<uint8_t>{}, 0));
            return response;
        }

        const auto [range_begin, range_end] = parseRange(request);
        const size_t length = range_end - range_begin + extra_bytes;

        std::vector<uint8_t> data(length);
        for (size_t i = 0; i < length; ++i)
            data[i] = static_cast<uint8_t>(range_begin + i);

        response->SetHeader("Content-Length", std::to_string(length));
        response->SetHeader(
            "Content-Range",
            "bytes " + std::to_string(range_begin) + "-" + std::to_string(range_begin + length - 1) + "/" + std::to_string(blob_size));
        response->SetBodyStream(std::make_unique<FixedBodyStream>(std::move(data), static_cast<int64_t>(length)));
        return response;
    }

private:
    /// `bytes=<begin>-<end>`, where `<end>` is inclusive and can be absent. Returns the
    /// half-open range.
    std::pair<size_t, size_t> parseRange(const Azure::Core::Http::Request & request) const
    {
        const auto headers = request.GetHeaders();
        auto it = headers.find("x-ms-range");
        if (it == headers.end())
            it = headers.find("range");
        if (it == headers.end())
            return {0, blob_size};

        const std::string & value = it->second;
        const size_t equals_pos = value.find('=');
        const size_t dash_pos = value.find('-', equals_pos + 1);

        const size_t range_begin = std::stoul(value.substr(equals_pos + 1, dash_pos - equals_pos - 1));
        if (dash_pos + 1 == value.size())
            return {range_begin, blob_size};

        return {range_begin, std::stoul(value.substr(dash_pos + 1)) + 1};
    }

    static constexpr size_t blob_size = 1024;
    size_t extra_bytes;
    std::string etag;
    bool honours_if_match;
    size_t downloads = 0;
    size_t properties_requests = 0;
    std::string last_if_match;
};

std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeCountingBuffer(size_t extra_bytes, size_t buffer_size)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<CountingRangeTransport>(extra_bytes);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        read_settings,
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1);
}

}

/// `supportsRightBoundedReads` promises that a bound set by `setReadUntilPosition` takes effect
/// immediately. Tightening it after a part of a wider download has already been buffered must not
/// hand out the bytes past the new bound that are still sitting in the working buffer.
TEST(AzureReadUntilPosition, TightenedAfterPartialRead)
{
    auto buffer = makeCountingBuffer(/* extra_bytes */ 0, /* buffer_size */ 64);
    buffer->setReadUntilPosition(100);

    std::array<char, 10> head{};
    ASSERT_EQ(buffer->read(head.data(), head.size()), head.size());
    ASSERT_EQ(buffer->getPosition(), static_cast<off_t>(10));

    buffer->setReadUntilPosition(20);

    std::string rest;
    ASSERT_NO_THROW(DB::readStringUntilEOF(rest, *buffer));

    ASSERT_EQ(rest.size(), static_cast<size_t>(10));
    for (size_t i = 0; i < rest.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(rest[i]), static_cast<uint8_t>(10 + i)) << "at position " << i;

    ASSERT_TRUE(buffer->eof());
    ASSERT_EQ(buffer->getPosition(), static_cast<off_t>(20));
}

/// The same, with an endpoint that answers every ranged request with more bytes than were
/// requested: neither the stale buffer nor the overlong response may cross the new bound.
TEST(AzureReadUntilPosition, TightenedAfterPartialReadWithOverlongResponse)
{
    auto buffer = makeCountingBuffer(/* extra_bytes */ 28, /* buffer_size */ 64);
    buffer->setReadUntilPosition(100);

    std::array<char, 10> head{};
    ASSERT_EQ(buffer->read(head.data(), head.size()), head.size());

    buffer->setReadUntilPosition(20);

    std::string rest;
    ASSERT_NO_THROW(DB::readStringUntilEOF(rest, *buffer));

    ASSERT_EQ(rest.size(), static_cast<size_t>(10));
    for (size_t i = 0; i < rest.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(rest[i]), static_cast<uint8_t>(10 + i)) << "at position " << i;

    ASSERT_TRUE(buffer->eof());
}

/// Widening the bound after a partial read must keep the already-read prefix and continue from the
/// position the caller has read up to.
TEST(AzureReadUntilPosition, WidenedAfterPartialRead)
{
    auto buffer = makeCountingBuffer(/* extra_bytes */ 0, /* buffer_size */ 64);
    buffer->setReadUntilPosition(20);

    std::array<char, 10> head{};
    ASSERT_EQ(buffer->read(head.data(), head.size()), head.size());

    buffer->setReadUntilPosition(200);

    std::string rest;
    ASSERT_NO_THROW(DB::readStringUntilEOF(rest, *buffer));

    ASSERT_EQ(rest.size(), static_cast<size_t>(190));
    for (size_t i = 0; i < rest.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(rest[i]), static_cast<uint8_t>(10 + i)) << "at position " << i;
}

namespace
{

/// The same as `makeCountingBuffer`, but also hands the transport back, so that a test can look at
/// the requests the reader has issued.
std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeCountingBuffer(
    size_t extra_bytes, size_t buffer_size, std::shared_ptr<CountingRangeTransport> & transport)
{
    transport = std::make_shared<CountingRangeTransport>(extra_bytes);

    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = transport;

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        read_settings,
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1);
}

}

/// The empty range `[0, 0)` is a bound like any other: `supportsRightBoundedReads` promises that
/// the reader stops at it, so it must report EOF right away instead of taking a bound of zero for
/// an unbounded read. It must not go to the endpoint either, because no byte of the response could
/// be handed out.
TEST(AzureReadUntilPosition, EmptyRange)
{
    std::shared_ptr<CountingRangeTransport> transport;
    auto buffer = makeCountingBuffer(/* extra_bytes */ 0, /* buffer_size */ 64, transport);
    buffer->setReadUntilPosition(0);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_TRUE(data.empty());
    ASSERT_TRUE(buffer->eof());
    ASSERT_EQ(transport->getDownloadCount(), static_cast<size_t>(0));
}

/// `setReadUntilEnd` is the way to say "no bound", and it must still work after the bound has been
/// set to the empty range.
TEST(AzureReadUntilPosition, EmptyRangeThenReadUntilEnd)
{
    auto buffer = makeCountingBuffer(/* extra_bytes */ 0, /* buffer_size */ 64);
    buffer->setReadUntilPosition(0);
    ASSERT_TRUE(buffer->eof());

    buffer->setReadUntilEnd();

    std::array<char, 10> head{};
    ASSERT_EQ(buffer->read(head.data(), head.size()), head.size());
    for (size_t i = 0; i < head.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(head[i]), static_cast<uint8_t>(i)) << "at position " << i;
}

/// A bound below the current position is allowed as long as the caller seeks back before reading,
/// which `ReadBuffer::setReadUntilPosition` explicitly recommends supporting. Tightening it all the
/// way down to the empty range is no different.
TEST(AzureReadUntilPosition, TightenedToEmptyRangeAfterPartialRead)
{
    std::shared_ptr<CountingRangeTransport> transport;
    auto buffer = makeCountingBuffer(/* extra_bytes */ 0, /* buffer_size */ 64, transport);
    buffer->setReadUntilPosition(100);

    std::array<char, 10> head{};
    ASSERT_EQ(buffer->read(head.data(), head.size()), head.size());

    const size_t downloads_after_head = transport->getDownloadCount();

    buffer->setReadUntilPosition(0);
    buffer->seek(0, SEEK_SET);

    std::string rest;
    ASSERT_NO_THROW(DB::readStringUntilEOF(rest, *buffer));

    ASSERT_TRUE(rest.empty());
    ASSERT_EQ(transport->getDownloadCount(), downloads_after_head);
}

/// An endpoint that honours the requested range must keep working: reading a bounded range from a
/// nonzero offset returns exactly the bytes of the blob at that offset.
TEST(AzureReadUntilPosition, SeekToNonZeroOffsetWithHonestEndpoint)
{
    auto buffer = makeCountingBuffer(/* extra_bytes */ 0, /* buffer_size */ 64);

    buffer->seek(100, SEEK_SET);
    buffer->setReadUntilPosition(200);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(100 + i)) << "at position " << i;
}

namespace
{

/// An `AzureObjectStorage` whose every request is answered by `RangeResponseTransport`, which
/// serves the blob from its beginning regardless of the requested range and reports
/// `served_size` as the length of the body.
std::unique_ptr<DB::AzureObjectStorage> makeObjectStorage(size_t claimed_size, size_t served_size)
{
    DB::AzureBlobStorage::ConnectionParams connection_params;
    connection_params.endpoint.container_name = "container";
    connection_params.client_options.Retry.MaxRetries = 0;
    connection_params.client_options.Transport.Transport
        = std::make_shared<RangeResponseTransport>(claimed_size, served_size);

    auto container_client = std::make_unique<DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", connection_params.client_options),
        /* blob_prefix */ "");

    return std::make_unique<DB::AzureObjectStorage>(
        "azure",
        std::move(container_client),
        std::make_unique<DB::AzureBlobStorage::RequestSettings>(),
        connection_params,
        /* object_namespace */ "container",
        /* description */ "azure",
        /* common_key_prefix */ "");
}

}

/// A whole-object read through `AzureObjectStorage::readObject` sets no right bound of its own, so
/// before `StoredObject::bytes_size` was threaded through as the bound, the size of the read came
/// from the length of the response. An endpoint answering a request for a 100-byte object with 128
/// bytes then handed the caller 28 bytes past the end of the object. The size from the metadata is
/// known locally, so it, and not the response, decides where the object ends.
TEST(AzureReadObject, BoundedByTheObjectSizeFromTheMetadata)
{
    auto object_storage = makeObjectStorage(/* claimed_size */ 128, /* served_size */ 128);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An object whose size was never determined carries the `UnknownSize` sentinel, which is not a
/// bound: such a read must still run to the end of whatever the endpoint returns rather than
/// stopping immediately or reading a sentinel-sized range.
TEST(AzureReadObject, UnknownObjectSizeReadsToTheEnd)
{
    auto object_storage = makeObjectStorage(/* claimed_size */ 100, /* served_size */ 100);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ DB::StoredObject::UnknownSize);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// `UnknownSize` is the only sentinel for an undetermined size, so `bytes_size == 0` describes a
/// genuinely empty object and is a bound like any other. Treating it as "no bound" made an empty
/// object read unbounded, so an endpoint answering with a non-empty body handed the caller bytes
/// of an object that the metadata says has none.
TEST(AzureReadObject, EmptyObjectReadsAsEmpty)
{
    auto object_storage = makeObjectStorage(/* claimed_size */ 128, /* served_size */ 128);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 0);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_TRUE(data.empty());
    ASSERT_TRUE(buffer->eof());
}

/// Bounding the read by `bytes_size` means an empty object is read without issuing a single
/// request, so the metadata of the last request does not exist. `readSmallObjectAndGetObjectMetadata`
/// must still return the metadata of the object instead of throwing `NOT_INITIALIZED`.
TEST(AzureReadObject, EmptyObjectStillReportsMetadata)
{
    auto object_storage = makeObjectStorage(/* claimed_size */ 0, /* served_size */ 0);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 0);

    DB::SmallObjectDataWithMetadata result;
    ASSERT_NO_THROW(result = object_storage->readSmallObjectAndGetObjectMetadata(object, DB::ReadSettings{}, /* max_size_bytes */ 4096));

    ASSERT_TRUE(result.data.empty());
    ASSERT_EQ(result.metadata.size_bytes, static_cast<size_t>(0));
    ASSERT_EQ(result.metadata.etag, "\"0x8DA000000000000\"");
}

namespace
{

/// An `AzureObjectStorage` whose every request is answered by `CountingRangeTransport`, so that the
/// blob has a generation (`etag`) and the endpoint either honours `If-Match` or ignores it.
std::unique_ptr<DB::AzureObjectStorage> makeCountingObjectStorage(std::shared_ptr<CountingRangeTransport> transport)
{
    DB::AzureBlobStorage::ConnectionParams connection_params;
    connection_params.endpoint.container_name = "container";
    connection_params.client_options.Retry.MaxRetries = 0;
    connection_params.client_options.Transport.Transport = std::move(transport);

    auto container_client = std::make_unique<DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", connection_params.client_options),
        /* blob_prefix */ "");

    return std::make_unique<DB::AzureObjectStorage>(
        "azure",
        std::move(container_client),
        std::make_unique<DB::AzureBlobStorage::RequestSettings>(),
        connection_params,
        /* object_namespace */ "container",
        /* description */ "azure",
        /* common_key_prefix */ "");
}

constexpr auto listed_etag = "\"0x8DA000000000000\"";
constexpr auto replaced_etag = "\"0x8DA000000000001\"";

/// Runs `action` and asserts that it fails with `AZURE_OBJECT_CHANGED_DURING_READ`.
template <typename Action>
void assertRejectsReplacedBlob(Action && action)
{
    try
    {
        action();
        FAIL() << "the read of a replaced blob succeeded";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::AZURE_OBJECT_CHANGED_DURING_READ) << e.message();
    }
}

}

/// `StoredObject::bytes_size` and `StoredObject::etag` describe one generation of the object. When
/// the blob has been replaced with a longer one after the listing, a read bounded by the stale size
/// alone would return a clean EOF after the first `bytes_size` bytes of the new generation. The
/// download is pinned to the listed generation with `If-Match`, so an honest endpoint rejects it
/// with `412 Precondition Failed`, which is reported as `AZURE_OBJECT_CHANGED_DURING_READ` and is
/// not retried as if it were transient.
TEST(AzureReadObject, RejectsReplacedBlobThroughIfMatch)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, replaced_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    object.etag = listed_etag;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    assertRejectsReplacedBlob([&] { DB::readStringUntilEOF(data, *buffer); });

    ASSERT_TRUE(data.empty());
    ASSERT_EQ(transport->getLastIfMatch(), listed_etag);
    ASSERT_EQ(transport->getDownloadCount(), static_cast<size_t>(1));
}

/// An endpoint that ignores `If-Match` and answers `206` with the current generation must not get
/// its bytes through either: the `ETag` of the response is checked against the listed one.
TEST(AzureReadObject, RejectsReplacedBlobWhenIfMatchIsIgnored)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, replaced_etag, /* honours_if_match */ false);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    object.etag = listed_etag;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    assertRejectsReplacedBlob([&] { DB::readStringUntilEOF(data, *buffer); });

    ASSERT_TRUE(data.empty());
}

/// The blob that was listed is the one that is read: the download carries the listed `ETag` as its
/// `If-Match` condition and the bytes come through, bounded by the listed size.
TEST(AzureReadObject, ReadsTheListedGeneration)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 28, listed_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    object.etag = listed_etag;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
    ASSERT_EQ(transport->getLastIfMatch(), listed_etag);
}

/// A caller that has not seen a generation of the blob sets no condition and gets whatever the
/// endpoint currently holds.
TEST(AzureReadObject, UnknownGenerationIsNotPinned)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, replaced_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    ASSERT_TRUE(transport->getLastIfMatch().empty());
}

/// An object listed as empty is read without a download, so no `If-Match` pins it; the `ETag` of
/// the properties that stand in for the response is checked against the listed one instead.
TEST(AzureReadObject, EmptyObjectMetadataRejectsReplacedBlob)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, replaced_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 0);
    object.etag = listed_etag;

    assertRejectsReplacedBlob([&] { object_storage->readSmallObjectAndGetObjectMetadata(object, DB::ReadSettings{}, /* max_size_bytes */ 4096); });
    ASSERT_EQ(transport->getDownloadCount(), static_cast<size_t>(0));
}

/// The plain `readObject` path of an object listed as empty issues no download either, so
/// `If-Match` never reaches the endpoint: the generation is checked on the properties before the
/// buffer is handed out, instead of returning the replaced blob as a clean empty file.
TEST(AzureReadObject, EmptyObjectRejectsReplacedBlob)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, replaced_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 0);
    object.etag = listed_etag;

    assertRejectsReplacedBlob([&] { object_storage->readObject(object, DB::ReadSettings{}); });
    ASSERT_EQ(transport->getDownloadCount(), static_cast<size_t>(0));
}

/// The XML body of a blob listing spells the `ETag` without the quotes that the `ETag` header of a
/// download has (`0x8DA...` against `"0x8DA..."`), so the two spellings name one generation and
/// must compare equal, and `If-Match` must carry the quoted spelling that HTTP prescribes.
constexpr auto unquoted_listed_etag = "0x8DA000000000000";

TEST(AzureReadObject, AcceptsTheUnquotedETagOfAListing)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, listed_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    object.etag = unquoted_listed_etag;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    ASSERT_EQ(transport->getLastIfMatch(), listed_etag);
}

TEST(AzureReadObject, EmptyObjectAcceptsTheUnquotedETagOfAListing)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, listed_etag, /* honours_if_match */ true);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 0);
    object.etag = unquoted_listed_etag;

    ASSERT_NO_THROW(object_storage->readObject(object, DB::ReadSettings{}));

    DB::SmallObjectDataWithMetadata result;
    ASSERT_NO_THROW(result = object_storage->readSmallObjectAndGetObjectMetadata(object, DB::ReadSettings{}, /* max_size_bytes */ 4096));
    ASSERT_TRUE(result.data.empty());
    ASSERT_EQ(result.metadata.etag, listed_etag);
    ASSERT_EQ(transport->getDownloadCount(), static_cast<size_t>(0));
}

/// `readObject` bounds the read by the size the caller recorded, so the buffer must report that
/// very size as the size of the file. Before the recorded size was handed to the buffer as its
/// `file_size`, `getFileSize` asked the endpoint with a live `GetProperties` request, whose answer
/// describes whatever generation the blob has by now: `CachedInMemoryReadBufferFromFile` sizes
/// itself by that answer in its constructor and throws `UNEXPECTED_END_OF_FILE` when the inner
/// buffer ends earlier, so a blob grown since it was listed turned a bounded read into an exception.
/// Here the endpoint reports the blob as empty on `HEAD`, while the caller has listed it as 100
/// bytes: the size the caller knows must win, and no request is needed to learn it.
TEST(AzureReadObject, FileSizeIsTheListedSize)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    ASSERT_EQ(buffer->getFileSize(), static_cast<size_t>(100));
    ASSERT_EQ(transport->getPropertiesCount(), static_cast<size_t>(0));

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
}

/// Without a recorded size there is nothing local to report, so the size is still learned from
/// the endpoint, as before.
TEST(AzureReadObject, UnknownObjectSizeIsAskedFromTheEndpoint)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0);
    auto object_storage = makeCountingObjectStorage(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ DB::StoredObject::UnknownSize);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    ASSERT_EQ(buffer->getFileSize(), static_cast<size_t>(0));
    ASSERT_EQ(transport->getPropertiesCount(), static_cast<size_t>(1));
}

TEST(AzureQuotedETag, Spellings)
{
    ASSERT_EQ(DB::ReadBufferFromAzureBlobStorage::quotedETag(""), "");
    ASSERT_EQ(DB::ReadBufferFromAzureBlobStorage::quotedETag("0x8DA000000000000"), "\"0x8DA000000000000\"");
    ASSERT_EQ(DB::ReadBufferFromAzureBlobStorage::quotedETag("\"0x8DA000000000000\""), "\"0x8DA000000000000\"");
    ASSERT_EQ(DB::ReadBufferFromAzureBlobStorage::quotedETag("\""), "\"\"\"");
}

/// `readBigAt` issues its own downloads, so it carries the same condition and the same check.
TEST(AzureReadBigAt, RejectsReplacedBlobThroughIfMatch)
{
    auto transport = std::make_shared<CountingRangeTransport>(/* extra_bytes */ 0, replaced_etag, /* honours_if_match */ true);

    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = transport;

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 3,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ std::nullopt,
        /* blob_storage_log */ nullptr,
        /* container_for_logging */ "",
        listed_etag);

    std::array<char, 64> out{};
    assertRejectsReplacedBlob([&] { buffer.readBigAt(out.data(), out.size(), /* range_begin */ 100, nullptr); });

    /// A replaced blob stays replaced, so the rejection is not retried.
    ASSERT_EQ(transport->getDownloadCount(), static_cast<size_t>(1));
}

#endif
