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
#include <Common/ProfileEvents.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
}

namespace ProfileEvents
{
    extern const Event ReadBufferFromAzureRequestsErrors;
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
/// response carries at most `max_response_size` bytes.
class BlobEndpoint : public Azure::Core::Http::HttpTransport
{
public:
    BlobEndpoint(size_t served_size_, size_t advertised_size_, size_t max_response_size_)
        : served_size(served_size_), advertised_size(advertised_size_), max_response_size(max_response_size_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        /// "x-ms-range: bytes=<start>-<end>", where "-<end>" is optional and `end` is inclusive.
        size_t range_start = 0;
        size_t range_limit = served_size;
        if (auto range = request.GetHeader("x-ms-range"); range.HasValue())
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

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");

        response->SetHeader("Content-Length", std::to_string(response_size));
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

/// Every increment of a profile event reaches `global_counters` through the chain of parents,
/// whichever counters the thread running the test has attached.
size_t requestErrors()
{
    return ProfileEvents::global_counters[ProfileEvents::ReadBufferFromAzureRequestsErrors];
}

void assertCountsFrom(const std::string & data, size_t first)
{
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(first + i)) << "at position " << first + i;
}

}

namespace
{

/// Reads until position `read_until_position` through a `buffer_size`-byte reading buffer.
std::string readWithRightBound(const std::shared_ptr<BlobEndpoint> & endpoint, size_t read_until_position, size_t buffer_size, size_t max_read_retries)
{
    auto buffer = makeBuffer(endpoint, buffer_size, max_read_retries);
    buffer->setReadUntilPosition(read_until_position);

    std::string result;
    DB::readStringUntilEOF(result, *buffer);
    return result;
}

}

/// The endpoint caps every response to 40 bytes, while the caller asked to read until position
/// 100 of a 100-byte blob. The end of a response is not the end of the data: the reader must reopen
/// the download at the offset it reached and reassemble all 100 bytes.
TEST(AzureBoundedRead, CappedResponsesAreReassembled)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 40);

    const size_t errors_before = requestErrors();

    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(endpoint, /* read_until_position */ 100, /* buffer_size */ 64, /* max_read_retries */ 4));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsFrom(data, 0);
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0, 40, 80}));
    /// The read is correct, only assembled from three responses: the reopens that continue it are
    /// not request errors, and they are not delayed by the backoff of the error path either.
    ASSERT_EQ(requestErrors(), errors_before);
}

/// The blob ends at 40 bytes no matter how often the download is reopened, while the caller asked
/// to read until position 100. A bounded read must either reach the right bound or fail - it must
/// not silently report the end of the file before the bound.
TEST(AzureBoundedRead, TruncatedBlobIsAnError)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 40, /* advertised_size */ 1000, /* max_response_size */ 40);

    const size_t errors_before = requestErrors();

    /// Expected to be set by an exception on a premature end of the response before the right bound.
    std::optional<int> error_code;
    try
    {
        readWithRightBound(endpoint, /* read_until_position */ 100, /* buffer_size */ 64, /* max_read_retries */ 3);
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }
    ASSERT_EQ(error_code, std::optional<int>(DB::ErrorCodes::UNEXPECTED_END_OF_FILE));
    /// Every retry reopened the download at the offset the read had reached.
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0, 40, 40}));
    /// A reopened download that hands out nothing is the error case, and it is counted as one.
    ASSERT_GT(requestErrors(), errors_before);
}

/// A response that delivers the whole requested range is not reopened.
TEST(AzureBoundedRead, ExactResponseIsNotReopened)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 100);

    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(endpoint, /* read_until_position */ 100, /* buffer_size */ 64, /* max_read_retries */ 4));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsFrom(data, 0);
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0}));
}

/// An unbounded read keeps its behaviour: the end of the response is the end of the file.
TEST(AzureBoundedRead, UnboundedReadEndsWithTheResponse)
{
    auto endpoint = std::make_shared<BlobEndpoint>(/* served_size */ 100, /* advertised_size */ 100, /* max_response_size */ 40);
    auto buffer = makeBuffer(endpoint, /* buffer_size */ 64, /* max_read_retries */ 4);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(40));
    ASSERT_EQ(endpoint->requested_offsets, (std::vector<size_t>{0}));
}

#endif
