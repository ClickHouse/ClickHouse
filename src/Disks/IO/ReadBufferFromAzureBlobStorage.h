#pragma once

#include <memory>
#include <mutex>
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
        std::optional<size_t> read_until_position_ = {},
        BlobStorageLogWriterPtr blob_storage_log_ = {},
        String container_for_logging_ = {});

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

    /// The offset just past the last byte that the current download is allowed to deliver.
    /// `reported_length` is the length of the response body as reported by the remote endpoint,
    /// and is not trusted: it is bounded by `read_until_position_`, which is set locally.
    static size_t getTotalSizeOfCurrentDownload(int64_t reported_length, off_t offset_, std::optional<size_t> read_until_position_);

    /// Creates the client on first use. Thread-safe.
    const AzureBlobStorage::BlobClient & getBlobClient() const;

    std::unique_ptr<Azure::Core::IO::BodyStream> data_stream;
    ContainerClientPtr blob_container_client;
    mutable BlobClientPtr blob_client;
    mutable std::once_flag blob_client_created;

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

    /// The offset just past the last byte the caller is allowed to read, when the caller has set
    /// a bound. An empty optional means "no bound", so that a bound of zero - the empty range
    /// `[0, 0)` - is honoured as a bound and reports EOF right away, as `supportsRightBoundedReads`
    /// promises, instead of being taken for an unbounded read.
    std::optional<size_t> read_until_position;

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
};

}

#endif
