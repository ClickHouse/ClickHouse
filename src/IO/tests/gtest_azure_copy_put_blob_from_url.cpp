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

/// A body stream that owns what it serves, see `gtest_azure_copy_blob_optional_properties.cpp`: the
/// transport policy of the SDK buffers the body unconditionally, so every response needs one.
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

/// Answers the three requests a cross-account native copy can make, telling them apart by the headers
/// the SDK sets for each (see `rest_client.cpp`):
///   `Copy Blob`            - PUT with `x-ms-copy-source` and no `x-ms-blob-type`
///   `Put Blob From URL`    - PUT with both
///   an ordinary upload     - PUT with `x-ms-blob-type` only, which is the read-and-write fallback
/// `Copy Blob` is always refused with 401, the answer to a source in another storage account.
class CrossAccountCopyTransport : public Azure::Core::Http::HttpTransport
{
public:
    explicit CrossAccountCopyTransport(bool accept_put_blob_from_url_) : accept_put_blob_from_url(accept_put_blob_from_url_) { }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        const auto & headers = request.GetHeaders();
        const bool has_copy_source = headers.count("x-ms-copy-source") != 0;
        const bool has_blob_type = headers.count("x-ms-blob-type") != 0;

        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Put && has_copy_source && !has_blob_type)
        {
            ++copies_started;
            return error(Azure::Core::Http::HttpStatusCode::Unauthorized, "Unauthorized", "AuthenticationFailed");
        }

        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Put && has_copy_source && has_blob_type)
        {
            ++puts_from_url;
            put_from_url_copy_source = headers.at("x-ms-copy-source");
            auto metadata = headers.find("x-ms-meta-answer");
            if (metadata != headers.end())
                put_from_url_metadata = metadata->second;

            if (!accept_put_blob_from_url)
            {
                /// What Azure answers when it cannot read the source of this request.
                return error(Azure::Core::Http::HttpStatusCode::InternalServerError, "Internal Server Error", "CannotVerifyCopySource");
            }

            auto response = created();
            return response;
        }

        /// The read half of the fallback.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Get)
        {
            ++source_reads;
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(
                1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");
            response->SetHeader("Content-Length", std::to_string(blob_size));
            response->SetHeader("Content-Range", "bytes 0-" + std::to_string(blob_size - 1) + "/" + std::to_string(blob_size));
            response->SetHeader("ETag", "\"0x8DA000000000001\"");
            response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-creation-time", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-blob-type", "BlockBlob");
            response->SetHeader("x-ms-lease-state", "available");
            response->SetHeader("x-ms-lease-status", "unlocked");
            response->SetBodyStream(std::make_unique<OwningBodyStream>(std::string(blob_size, 'x')));
            return response;
        }

        /// The write half of the fallback: a single-part upload, or a staged block and its block list.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Put)
        {
            ++fallback_writes;
            return created();
        }

        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Head)
        {
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
            response->SetHeader("Content-Length", std::to_string(blob_size));
            response->SetHeader("ETag", "\"0x8DA000000000001\"");
            response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-creation-time", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("x-ms-blob-type", "BlockBlob");
            response->SetHeader("x-ms-lease-state", "available");
            response->SetHeader("x-ms-lease-status", "unlocked");
            response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
            return response;
        }

        ++unexpected_requests;
        return error(Azure::Core::Http::HttpStatusCode::NotFound, "Not Found", "BlobNotFound");
    }

    size_t copies_started = 0;
    size_t puts_from_url = 0;
    size_t source_reads = 0;
    size_t fallback_writes = 0;
    size_t unexpected_requests = 0;
    std::string put_from_url_copy_source;
    std::string put_from_url_metadata;

private:
    static std::unique_ptr<Azure::Core::Http::RawResponse> created()
    {
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Created, "Created");
        response->SetHeader("Content-Length", "0");
        response->SetHeader("ETag", "\"0x8DA000000000001\"");
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
        return response;
    }

    static std::unique_ptr<Azure::Core::Http::RawResponse>
    error(Azure::Core::Http::HttpStatusCode code, const std::string & reason, const std::string & error_code)
    {
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, code, reason);
        response->SetHeader("Content-Length", "0");
        response->SetHeader("x-ms-error-code", error_code);
        response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
        return response;
    }

    bool accept_put_blob_from_url;
};

std::shared_ptr<const DB::AzureBlobStorage::ContainerClient> makeContainerClient(
    const std::shared_ptr<Azure::Core::Http::HttpTransport> & transport)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = transport;

    return std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");
}

/// A blob of at least `max_single_part_copy_size` bytes takes the asynchronous `Copy Blob`, which is
/// the only request the retry applies to.
std::shared_ptr<DB::AzureBlobStorage::RequestSettings> makeSettings(size_t max_put_blob_from_url_size)
{
    auto settings = std::make_shared<DB::AzureBlobStorage::RequestSettings>();
    settings->use_native_copy = true;
    settings->max_single_part_copy_size = blob_size;
    settings->max_put_blob_from_url_size = max_put_blob_from_url_size;
    return settings;
}

void copy(
    const std::shared_ptr<const DB::AzureBlobStorage::ContainerClient> & container_client,
    const std::shared_ptr<DB::AzureBlobStorage::RequestSettings> & settings)
{
    DB::copyAzureBlobStorageFile(
        container_client,
        container_client,
        /* src_container_for_logging */ "container",
        /* src_blob */ "blob",
        /* src_size */ blob_size,
        /* dest_container_for_logging */ "container",
        /* dest_blob */ "copy",
        settings,
        DB::ReadSettings{},
        /* object_to_attributes */ std::map<std::string, std::string>{{"answer", "42"}});
}

}

/// `azure_max_put_blob_from_url_size` is 0 by default, and then a refused copy is read through the
/// server as it was before the retry existed.
TEST(AzureNativeCopy, PutBlobFromUrlIsNotUsedByDefault)
{
    auto transport = std::make_shared<CrossAccountCopyTransport>(/* accept_put_blob_from_url */ true);
    ASSERT_NO_THROW(copy(makeContainerClient(transport), makeSettings(/* max_put_blob_from_url_size */ 0)));

    ASSERT_EQ(transport->copies_started, 1u);
    ASSERT_EQ(transport->puts_from_url, 0u);
    ASSERT_GE(transport->source_reads, 1u);
    ASSERT_EQ(transport->unexpected_requests, 0u);
}

/// With the limit set, the copy refused with 401 is retried as one `Put Blob From URL` carrying the
/// source and the requested metadata, and the object is not read through the server at all.
TEST(AzureNativeCopy, RetriesUnauthorizedCopyAsPutBlobFromUrl)
{
    auto transport = std::make_shared<CrossAccountCopyTransport>(/* accept_put_blob_from_url */ true);
    ASSERT_NO_THROW(copy(makeContainerClient(transport), makeSettings(/* max_put_blob_from_url_size */ blob_size)));

    ASSERT_EQ(transport->copies_started, 1u);
    ASSERT_EQ(transport->puts_from_url, 1u);
    ASSERT_EQ(transport->source_reads, 0u);
    ASSERT_EQ(transport->fallback_writes, 0u);
    ASSERT_EQ(transport->unexpected_requests, 0u);
    ASSERT_NE(transport->put_from_url_copy_source.find("blob"), std::string::npos);
    ASSERT_EQ(transport->put_from_url_metadata, "42");
}

/// An object larger than the limit is not retried, so the limit really bounds how long one object can
/// hold the thread that sent the synchronous request.
TEST(AzureNativeCopy, PutBlobFromUrlIsNotUsedAboveTheLimit)
{
    auto transport = std::make_shared<CrossAccountCopyTransport>(/* accept_put_blob_from_url */ true);
    ASSERT_NO_THROW(copy(makeContainerClient(transport), makeSettings(/* max_put_blob_from_url_size */ blob_size - 1)));

    ASSERT_EQ(transport->copies_started, 1u);
    ASSERT_EQ(transport->puts_from_url, 0u);
    ASSERT_GE(transport->source_reads, 1u);
    ASSERT_EQ(transport->unexpected_requests, 0u);
}

/// A failed retry must leave the copy no worse off than not retrying it: the original 401 is raised
/// again, so the object is still read through the server instead of the copy failing.
TEST(AzureNativeCopy, FallsBackToTheServerWhenPutBlobFromUrlFails)
{
    auto transport = std::make_shared<CrossAccountCopyTransport>(/* accept_put_blob_from_url */ false);
    ASSERT_NO_THROW(copy(makeContainerClient(transport), makeSettings(/* max_put_blob_from_url_size */ blob_size)));

    ASSERT_EQ(transport->copies_started, 1u);
    ASSERT_EQ(transport->puts_from_url, 1u);
    ASSERT_GE(transport->source_reads, 1u);
    ASSERT_EQ(transport->unexpected_requests, 0u);
}

#endif
