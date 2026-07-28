#pragma once

#include <memory>
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Common/MultiVersion.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WithFileName.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>

namespace DB
{

class BlobStorageLogWriter;
using BlobStorageLogWriterPtr = std::shared_ptr<BlobStorageLogWriter>;

class ReadBufferFromAzureBlobStorage : public ReadBufferFromFileBase
{
public:
    using ContainerClientPtr = std::shared_ptr<const AzureBlobStorage::ContainerClient>;
    using BlobClientPtr = std::unique_ptr<const AzureBlobStorage::BlobClient>;

    ReadBufferFromAzureBlobStorage(
        ContainerClientPtr blob_container_client_,
        const String & path_,
        const ReadSettings & read_settings_,
        size_t max_single_read_retries_,
        size_t max_single_download_retries_,
        bool use_external_buffer_ = false,
        bool restricted_seek_ = false,
        size_t read_until_position_ = 0,
        BlobStorageLogWriterPtr blob_storage_log_ = {},
        String container_for_logging_ = {},
        String expected_etag_ = {});

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    bool nextImpl() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    String getFileName() const override { return path; }

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    bool supportsRightBoundedReads() const override { return true; }

    std::optional<size_t> tryGetFileSize() override;

    std::optional<RemoteFileMetadata> getRemoteFileMetadata() const override;

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    bool supportsReadAt() override { return true; }

    /// nextImpl fills the caller's set() buffer only when built for external-buffer use.
    bool supportsExternalBufferMode() const override { return use_external_buffer; }

    /// Buffer may issue several requests, so theoretically metadata may be different for different requests.
    /// This method returns metadata from the last request. If there were no requests, it will throw exception.
    ObjectMetadata getObjectMetadataFromTheLastRequest() const;

private:
    void initialize(size_t attempt);
    void setMetadataFromResponse(const Azure::Storage::Blobs::Models::DownloadBlobDetails & details, size_t blob_size) const;

    /// Pin the download to the generation of the blob that was observed when the object was listed
    /// (`If-Match`), so that a concurrent in-place rewrite makes the request fail instead of serving
    /// bytes of a different generation. A single logical read issues several `Download` requests
    /// (initial download, retries, `readBigAt`), so without the pin they can be stitched together
    /// from two generations. No-op when no expected ETag is known.
    void pinToExpectedEtag(Azure::Storage::Blobs::DownloadBlobOptions & download_options) const;

    /// Belt and braces for the `If-Match` pin: Azure is expected to answer `412 Precondition Failed`,
    /// but validating the ETag that came back costs nothing and also covers endpoints (emulators,
    /// gateways) that ignore the condition header.
    void validateResponseEtag(const Azure::Storage::Blobs::Models::DownloadBlobDetails & details) const;

    /// Translate the `412` produced by the `If-Match` pin into the same non-retryable error as the
    /// response-ETag check, instead of letting it surface as a generic Azure request failure.
    void rethrowIfEtagPinFailed(const Azure::Core::RequestFailedException & e) const;

    std::unique_ptr<Azure::Core::IO::BodyStream> data_stream;
    ContainerClientPtr blob_container_client;
    BlobClientPtr blob_client;

    const String path;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
    ReadSettings read_settings;
    std::vector<char> tmp_buffer;
    size_t tmp_buffer_size;
    bool use_external_buffer;

    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;

    off_t read_until_position = 0;

    off_t offset = 0;
    size_t total_size{};
    bool initialized = false;
    char * data_ptr;
    size_t data_capacity;

    LoggerPtr log = getLogger("ReadBufferFromAzureBlobStorage");
    /// No-way to make metadata non-mutable, because readBig method is const.
    mutable MultiVersion<std::optional<ObjectMetadata>> last_object_metadata;

    mutable BlobStorageLogWriterPtr blob_storage_log;
    String container_for_logging;

    /// ETag observed when the object was listed; empty when unknown or when validation is disabled.
    String expected_etag;
};

}

#endif
