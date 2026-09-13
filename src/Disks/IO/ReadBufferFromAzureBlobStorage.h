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

/// Copies at most `n` bytes from `body_stream` into `to` and returns the number of bytes copied.
/// The length of the stream is the `Content-Length` reported by the remote endpoint and is not
/// consulted at all: an endpoint that returns more data than the requested range would otherwise
/// overflow the destination buffer, and one that reports less than the body actually holds would
/// truncate the copy. The copy stops at `n` bytes or at the actual end of the body.
size_t copyFromAzureBodyStream(Azure::Core::IO::BodyStream & body_stream, char * to, size_t n, const Azure::Core::Context & context);

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
        std::optional<size_t> known_object_size_ = {},
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

    /// The offset just past the last byte of the data that the read is expected to deliver.
    size_t getEndOfData() const;

    /// Whether the end of the data is known from local information rather than from the responses
    /// of this read.
    bool isEndOfDataKnownLocally() const { return read_until_position != 0 || known_object_size.has_value(); }

    /// Drops the current response and the bytes buffered from it, so that the next read reopens
    /// the download at the current position. Called when the right bound of the read changes.
    void discardCurrentDownload();

    void setMetadataFromResponse(const Azure::Storage::Blobs::Models::DownloadBlobDetails & details, size_t blob_size) const;

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

    /// The size of the object as it was known locally before the read started (from the `LIST` or
    /// the `HEAD` that produced the `StoredObject`). It does not come from the response that is
    /// being validated, so it is a hard end of the data of an unbounded read: every layer above
    /// this buffer already treats it as the length of the file - `ReadBufferFromRemoteFSGather`
    /// places the following object right behind it, `ReaderExecutor` clamps object reads to it, the
    /// caches address the data within a file of that length, and `AsynchronousBoundedReadBuffer`
    /// takes it as its own right bound - so a response that carries more data than that cannot have
    /// the excess handed to the caller under offsets that belong to something else.
    std::optional<size_t> known_object_size;

    /// The `ETag` of the object generation selected at read setup. When it is not empty, every
    /// `Download` (including a reopen after a premature end of the response) is pinned to it with
    /// `If-Match`, and the `ETag` of the response is compared with it as defence in depth, so that
    /// one logical read cannot be stitched together from two generations of the blob.
    String expected_etag;

    /// The largest size of the whole object advertised by the `Content-Range` of any download
    /// response of the current logical read. It is remote data, so it is only consulted for an
    /// unbounded read whose size is not known locally, and only as a lower bound: a response body
    /// that ends before it is a premature end of the response rather than the end of the file. It
    /// never decreases within one logical read and is reset by a seek.
    size_t reported_object_size = 0;

    /// A download was freshly opened at the current offset in order to find out whether the object
    /// really ends there, for a read that has no locally known end of data. Reset as soon as any
    /// byte arrives, by a seek and by a change of the right bound.
    bool end_of_object_probed = false;

    /// The endpoint refused a range that starts at the current offset, which is it stating that the
    /// object has no byte there. Only set for a read with no locally known end of data, and only at
    /// an offset the responses of this read did not claim to be inside the object.
    bool end_of_object_confirmed = false;

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
