#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <cstring>
#include <memory>
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
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// A body stream that owns what it serves.
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

/// A fake Azure endpoint holding one blob whose byte at position `i` is `i` (modulo 256), so that
/// every byte a reader receives can be attributed to its position in the blob. The blob holds
/// `served_size` bytes and is advertised as `advertised_size` bytes long in `Content-Range`. Every
/// response carries at most `max_response_size` bytes. With `ignore_range`, the endpoint answers
/// every request with `200 OK` and the blob from byte 0, the way an endpoint or a proxy that does
/// not understand ranges does.
class BlobEndpoint : public Azure::Core::Http::HttpTransport
{
public:
    BlobEndpoint(size_t served_size_, size_t advertised_size_, size_t max_response_size_, bool ignore_range_ = false)
        : served_size(served_size_), advertised_size(advertised_size_), max_response_size(max_response_size_), ignore_range(ignore_range_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        /// "x-ms-range: bytes=<start>-<end>", where "-<end>" is optional and `end` is inclusive.
        size_t range_start = 0;
        size_t range_limit = served_size;
        if (auto range = request.GetHeader("x-ms-range"); range.HasValue() && !ignore_range)
        {
            const std::string & value = range.Value();
            if (const size_t eq_pos = value.find('='); eq_pos != std::string::npos)
            {
                range_start = std::stoull(value.substr(eq_pos + 1));
                if (const size_t dash_pos = value.find('-', eq_pos + 1); dash_pos != std::string::npos && dash_pos + 1 < value.size())
                    range_limit = std::min(range_limit, static_cast<size_t>(std::stoull(value.substr(dash_pos + 1))) + 1);
            }
        }
        requested_offsets.push_back(range_start);

        const size_t response_size = range_start < range_limit ? std::min(max_response_size, range_limit - range_start) : 0;
        const size_t range_end = range_start + (response_size == 0 ? 0 : response_size - 1);

        auto response = ignore_range
            ? std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK")
            : std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");

        response->SetHeader("Content-Length", std::to_string(response_size));
        /// A `200 OK` response carries no `Content-Range`.
        if (!ignore_range)
            response->SetHeader(
                "Content-Range",
                "bytes " + std::to_string(range_start) + "-" + std::to_string(range_end) + "/" + std::to_string(advertised_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetHeader("ETag", "\"0x8DA000000000000\"");
        response->SetHeader("x-ms-blob-type", "BlockBlob");

        std::vector<uint8_t> data(response_size);
        for (size_t i = 0; i < response_size; ++i)
            data[i] = static_cast<uint8_t>(range_start + i);
        response->SetBodyStream(std::make_unique<OwningBodyStream>(std::move(data)));
        return response;
    }

    /// The offset every download request asked for, in order.
    std::vector<size_t> requested_offsets;

private:
    size_t served_size;
    size_t advertised_size;
    size_t max_response_size;
    bool ignore_range;
};

std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeBuffer(
    const std::shared_ptr<BlobEndpoint> & endpoint, size_t buffer_size = DB::DBMS_DEFAULT_BUFFER_SIZE, size_t max_read_retries = 1)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = endpoint;

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1);
}

void assertCountsFrom(const std::string & data, size_t first)
{
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(first + i)) << "at position " << first + i;
}

}

/// The reader has already been handed bytes 0..99 by a single response when the caller consumes
/// 32 of them and then lowers the right bound to 40. The bytes 40..99 that the working buffer still
/// holds are past the new bound and must not be delivered: the read must end at byte 40 exactly.
TEST(AzureReadUntilPositionChange, BoundLoweredBelowBufferedBytes)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 100);
    auto buffer = makeBuffer(endpoint);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    assertCountsFrom(head, 0);

    buffer->setReadUntilPosition(40);

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(8));
    assertCountsFrom(tail, 32);
    ASSERT_EQ(buffer->getPosition(), 40);
    /// The bytes up to the new bound were already buffered, so no download was reopened.
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0}));
}

/// The right bound is raised while the reader still holds bytes from the response of the previous
/// bound: the response answered the range 0..63, the caller consumes 32 bytes and asks to read
/// until byte 80. The buffered bytes 32..63 lie within the new bound and are kept; the download is
/// reopened after them, under the new bound, and the read delivers exactly bytes 32..79.
TEST(AzureReadUntilPositionChange, BoundRaisedAfterBufferedBytes)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 100);
    auto buffer = makeBuffer(endpoint);
    buffer->setReadUntilPosition(64);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    assertCountsFrom(head, 0);

    buffer->setReadUntilPosition(80);

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(48));
    assertCountsFrom(tail, 32);
    ASSERT_EQ(buffer->getPosition(), 80);
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0, 64}));
}

/// Setting the same right bound again is not a new logical read: nothing already buffered is
/// dropped and the download is not reopened.
TEST(AzureReadUntilPositionChange, SameBoundSetTwice)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 100);
    auto buffer = makeBuffer(endpoint);
    buffer->setReadUntilPosition(64);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    ASSERT_EQ(buffer->available(), static_cast<size_t>(32));

    buffer->setReadUntilPosition(64);
    ASSERT_EQ(buffer->available(), static_cast<size_t>(32));

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(32));
    assertCountsFrom(tail, 32);
    ASSERT_EQ(buffer->getPosition(), 64);
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0}));
}

/// Two bound changes before the next read: the first raises the bound and keeps the buffered
/// bytes 32..63 of the response for 0..63, the second lowers it to 40 before anything else is read.
/// The second change must still cut the buffered bytes past 40, although the download was already
/// closed by the first one.
TEST(AzureReadUntilPositionChange, BoundRaisedThenLoweredBeforeNextRead)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 100);
    auto buffer = makeBuffer(endpoint);
    buffer->setReadUntilPosition(64);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    assertCountsFrom(head, 0);

    buffer->setReadUntilPosition(80);
    buffer->setReadUntilPosition(40);

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(8));
    assertCountsFrom(tail, 32);
    ASSERT_EQ(buffer->getPosition(), 40);
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0}));
}

#endif
