#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <IO/ReadSettings.h>
#include <Common/logger_useful.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace
{

constexpr size_t blob_size = 100;

/// A body stream that owns what it serves. Every response needs one: the transport policy of the
/// SDK buffers the body by calling `ReadToEnd` on it unconditionally, so a response without a body
/// stream dereferences a null pointer - including the answer to a HEAD request, which has no body.
class OwningBodyStream : public Azure::Core::IO::BodyStream
{
public:
    explicit OwningBodyStream(std::string data_) : data(std::move(data_)) { }

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

    std::string data;
    size_t position = 0;
};

/// An endpoint that accepts an asynchronous `Copy Blob` (`StartCopyFromUri`) and reports it as
/// completed in the polled properties of the destination, without the optional `x-ms-copy-source`
/// header. The SDK models `BlobProperties::CopySource` as `Nullable`, and `Nullable::Value()` of an
/// empty one aborts the process in a release build (`AZURE_ASSERT_MSG` expands to a bare
/// `std::abort` under `NDEBUG`), so an endpoint behaving this way must not be able to take the
/// server down.
class CopyWithoutCopySourceTransport : public Azure::Core::Http::HttpTransport
{
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        /// `Copy Blob`: a `PUT` of the destination with `x-ms-copy-source`. Accepted, still pending.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Put)
        {
            ++copies_started;
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Accepted, "Accepted");
            response->SetHeader("Content-Length", "0");
            response->SetHeader("ETag", "\"0x8DA000000000001\"");
            response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-copy-id", "copy-id");
            response->SetHeader("x-ms-copy-status", "pending");
            response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
            return response;
        }

        /// The properties of the destination, polled until the copy completes. These are the headers
        /// the SDK reads from such a response; `x-ms-copy-source` is left out on purpose.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Head)
        {
            ++properties_polled;
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
            response->SetHeader("Content-Length", std::to_string(blob_size));
            response->SetHeader("ETag", "\"0x8DA000000000001\"");
            response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-creation-time", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-blob-type", "BlockBlob");
            response->SetHeader("x-ms-lease-state", "available");
            response->SetHeader("x-ms-lease-status", "unlocked");
            response->SetHeader("x-ms-server-encrypted", "true");
            response->SetHeader("x-ms-copy-id", "copy-id");
            response->SetHeader("x-ms-copy-status", "success");
            response->SetHeader("x-ms-copy-progress", std::to_string(blob_size) + "/" + std::to_string(blob_size));
            response->SetHeader("x-ms-copy-completion-time", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
            return response;
        }

        /// Anything else would be the read-and-write fallback, which must not be reached: the native
        /// copy completed.
        ++unexpected_requests;
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::NotFound, "Not Found");
        response->SetHeader("Content-Length", "0");
        response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
        return response;
    }

    size_t copies_started = 0;
    size_t properties_polled = 0;
    size_t unexpected_requests = 0;
};

}

TEST(AzureNativeCopy, CompletionWithoutTheCopySourceInTheProperties)
{
    /// The copy source is logged at the trace level, and a `LOG_TRACE` evaluates its arguments only
    /// when the logger is at that level - which is the default level of the server, so this is the
    /// configuration in which the property is dereferenced.
    getLogger("copyAzureBlobStorageFile")->setLevel("trace");

    auto transport = std::make_shared<CopyWithoutCopySourceTransport>();

    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = transport;

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    auto settings = std::make_shared<DB::AzureBlobStorage::RequestSettings>();
    settings->use_native_copy = true;
    /// A blob of at least `max_single_part_copy_size` bytes is copied with the asynchronous `Copy Blob`,
    /// whose completion is read from the polled properties.
    settings->max_single_part_copy_size = blob_size;

    ASSERT_NO_THROW(DB::copyAzureBlobStorageFile(
        container_client,
        container_client,
        /* src_container_for_logging */ "container",
        /* src_blob */ "blob",
        /* src_size */ blob_size,
        /* dest_container_for_logging */ "container",
        /* dest_blob */ "copy",
        settings,
        DB::ReadSettings{},
        /* object_to_attributes */ std::nullopt));

    ASSERT_EQ(transport->copies_started, 1u);
    ASSERT_GE(transport->properties_polled, 1u);
    ASSERT_EQ(transport->unexpected_requests, 0u);
}

#endif
