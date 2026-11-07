#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>

#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/ReadHelpers.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <gtest/gtest.h>

namespace
{

/// The content of the single blob the fake endpoint serves.
const std::string blob_content = "0123456789";

/// A listing of a single blob whose `Properties` carry no `Etag`. Every element of `Properties` is
/// optional in the response schema, so this is a well-formed answer.
const std::string blob_list_without_etag = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Blobs>
    <Blob>
      <Name>blob</Name>
      <Properties>
        <Last-Modified>Wed, 21 Oct 2015 07:28:00 GMT</Last-Modified>
        <Content-Length>10</Content-Length>
      </Properties>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>)";

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

/// An endpoint that answers `GetProperties`, `ListBlobs` and `Download` without the optional `ETag`.
/// `Azure::ETag::ToString` aborts the process when the tag is absent - in release builds too,
/// because `AZURE_ASSERT_MSG` expands to a bare `std::abort` under `NDEBUG` - so an endpoint
/// behaving this way must not be able to take the server down.
class NoETagTransport : public Azure::Core::Http::HttpTransport
{
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
        Azure::Core::Http::Request & request, const Azure::Core::Context &) override
    {
        /// `GetProperties` is a HEAD request and carries everything in the headers.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Head)
        {
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
            response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("Content-Length", std::to_string(blob_content.size()));
            response->SetHeader("x-ms-blob-type", "BlockBlob");
            response->SetBodyStream(std::make_unique<OwningBodyStream>(""));
            return response;
        }

        /// `ListBlobs` is a GET on the container with `comp=list`.
        if (request.GetUrl().GetQueryParameters().contains("comp"))
        {
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
            response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            response->SetHeader("Content-Type", "application/xml");
            response->SetHeader("Content-Length", std::to_string(blob_list_without_etag.size()));
            response->SetBodyStream(std::make_unique<OwningBodyStream>(blob_list_without_etag));
            return response;
        }

        /// Everything else is a ranged `Download` of the blob. Answer it with the whole blob.
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        response->SetHeader("Content-Length", std::to_string(blob_content.size()));
        response->SetHeader(
            "Content-Range", "bytes 0-" + std::to_string(blob_content.size() - 1) + "/" + std::to_string(blob_content.size()));
        response->SetHeader("x-ms-blob-type", "BlockBlob");
        response->SetBodyStream(std::make_unique<OwningBodyStream>(blob_content));
        return response;
    }
};

Azure::Storage::Blobs::BlobContainerClient createRawContainerClientWithoutETag()
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<NoETagTransport>();
    return Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options);
}

std::unique_ptr<DB::AzureObjectStorage> createObjectStorageWithoutETag()
{
    auto container_client = std::make_unique<DB::AzureBlobStorage::ContainerClient>(
        createRawContainerClientWithoutETag(), /* blob_prefix */ "");

    return std::make_unique<DB::AzureObjectStorage>(
        "azure",
        std::move(container_client),
        std::make_unique<DB::AzureBlobStorage::RequestSettings>(),
        DB::AzureBlobStorage::ConnectionParams{},
        /* object_namespace */ "container",
        /* description */ "http://azure.invalid/container",
        /* common_key_prefix */ "");
}

std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> createReadBufferWithoutETag()
{
    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        createRawContainerClientWithoutETag(), /* blob_prefix */ "");

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1);
}

}

/// `GetProperties` is answered without an `ETag`: the metadata must come back with an empty tag
/// instead of aborting the process.
TEST(AzureObjectStorageMetadata, GetObjectMetadataWithoutETag)
{
    auto object_storage = createObjectStorageWithoutETag();

    DB::ObjectMetadata metadata;
    ASSERT_NO_THROW(metadata = object_storage->getObjectMetadata("blob", /* with_tags */ false));

    ASSERT_EQ(metadata.etag, "");
    ASSERT_EQ(metadata.size_bytes, blob_content.size());
}

/// The same for the listing path, where the tag of every blob comes from the response body.
TEST(AzureObjectStorageMetadata, ListObjectsWithoutETag)
{
    auto object_storage = createObjectStorageWithoutETag();

    DB::RelativePathsWithMetadata children;
    ASSERT_NO_THROW(object_storage->listObjects("", children, /* max_keys */ 1));

    ASSERT_EQ(children.size(), static_cast<size_t>(1));
    ASSERT_EQ(children[0]->relative_path, "blob");
    ASSERT_EQ(children[0]->metadata->etag, "");
}

/// And for the iterator, which builds the same metadata on its own.
TEST(AzureObjectStorageMetadata, IterateWithoutETag)
{
    auto object_storage = createObjectStorageWithoutETag();

    DB::ObjectStorageIteratorPtr iterator;
    ASSERT_NO_THROW(iterator = object_storage->iterate("", /* max_keys */ 1, /* with_tags */ false, /* start_after */ {}));

    ASSERT_TRUE(iterator->isValid());
    ASSERT_EQ(iterator->current()->relative_path, "blob");
    ASSERT_EQ(iterator->current()->metadata->etag, "");
}

/// A `Download` answered without an `ETag`, on the sequential read path: the read must succeed
/// and the metadata of the last request must carry an empty tag.
TEST(AzureObjectStorageMetadata, ReadWithoutETag)
{
    auto buffer = createReadBufferWithoutETag();

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));

    ASSERT_EQ(data, blob_content);
    ASSERT_EQ(buffer->getObjectMetadataFromTheLastRequest().etag, "");
}

/// The same on the positioned read path, which converts the tag of its own response.
TEST(AzureObjectStorageMetadata, ReadBigAtWithoutETag)
{
    auto buffer = createReadBufferWithoutETag();
    ASSERT_TRUE(buffer->supportsReadAt());

    std::string data(blob_content.size(), '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(data.data(), data.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, blob_content.size());
    ASSERT_EQ(data, blob_content);
    ASSERT_EQ(buffer->getObjectMetadataFromTheLastRequest().etag, "");
}

#endif
