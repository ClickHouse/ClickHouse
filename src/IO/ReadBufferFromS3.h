#pragma once

#include <IO/S3Settings.h>
#include "config.h"

#if USE_AWS_S3

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/S3/ReadBufferFromGetObjectResult.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <aws/s3/model/GetObjectResult.h>

namespace DB
{

class BlobStorageLogWriter;
using BlobStorageLogWriterPtr = std::shared_ptr<BlobStorageLogWriter>;

/**
 * Perform S3 HTTP GET request and provide response to read.
 */
class ReadBufferFromS3 : public ReadBufferFromFileBase
{
private:
    mutable std::shared_ptr<const S3::Client> client_ptr;
    String bucket;
    String key;
    String version_id;
    const S3::S3RequestSettings request_settings;

    /// These variables are atomic because they can be used for `logging only`
    /// (where it is not important to get consistent result)
    /// from separate thread other than the one which uses the buffer for s3 reading.
    std::atomic<off_t> offset = 0;
    std::atomic<off_t> read_until_position = 0;
    std::string stop_reason;
    std::string release_reason;

    std::unique_ptr<S3::ReadBufferFromGetObjectResult> impl;

    LoggerPtr log = getLogger("ReadBufferFromS3");

public:
    using S3CredentialsRefreshCallback = std::function<std::unique_ptr<const S3::Client>()>;

    ReadBufferFromS3(
        std::shared_ptr<const S3::Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const String & version_id_,
        const S3::S3RequestSettings & request_settings_,
        const ReadSettings & settings_,
        bool use_external_buffer = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false,
        std::optional<size_t> file_size = std::nullopt,
        const S3CredentialsRefreshCallback & credentials_refresh_callback_ = [] {return nullptr;},
        BlobStorageLogWriterPtr blob_storage_log_ = {}
        );

    ~ReadBufferFromS3() override = default;

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> tryGetFileSize() override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    bool supportsRightBoundedReads() const override { return true; }

    String getFileName() const override { return bucket + "/" + key; }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    bool supportsReadAt() override { return true; }

    /// Buffer may issue several requests, so theoretically metadata may be different for different requests.
    /// This method returns metadata from the last request. If there were no requests, it will throw exception.
    ObjectMetadata getObjectMetadataFromTheLastRequest() const;

    /// True when bytes already delivered to the consumer came from a response whose ETag turned out to
    /// differ from a later, reissued response's ETag, i.e. the bytes this buffer produced may come from
    /// more than one incarnation of the object. A response that never delivered a byte (e.g. the GET
    /// succeeded but the body read failed before any data arrived) does not count: reissuing it and
    /// getting a different ETag is an ordinary retry, not a coherence problem.
    bool responseIdentityChanged() const { return response_identity_changed; }

    size_t getReadUntilPosition() const { return read_until_position; }

    std::string getStopReason() const { return stop_reason; }

    std::optional<RemoteFileMetadata> getRemoteFileMetadata() const override;

private:
    std::unique_ptr<S3::ReadBufferFromGetObjectResult> initialize(size_t attempt);

    /// If true, if we destroy impl now, no work was wasted. Just for metrics.
    bool atEndOfRequestedRangeGuess();

    /// Call inside catch() block if GetObject fails. Bumps metrics, logs the error.
    /// Returns true if the error looks retriable.
    bool processException(size_t read_offset, size_t attempt) const;

    size_t getObjectSizeFromS3() const;

    Aws::S3::Model::GetObjectResult sendRequest(size_t attempt, size_t range_begin, std::optional<size_t> range_end_incl) const;

    /// Drops the identity baseline. Called when the next request is a reissue for a range the caller
    /// explicitly repositioned to (seek, or a change of the read-until bound), as opposed to a retry of
    /// the same range after a failure: the bytes already delivered before the reposition reached the
    /// consumer as their own self-consistent range, so the next response is not compared against them.
    void forgetResponseIdentityBaseline();

    /// ETag of the last response that has delivered at least one byte to the consumer: the baseline a
    /// newly-delivering response is checked against. A response that never delivers a byte (e.g. it
    /// fails before the body starts) leaves this untouched, however many such empty attempts happen in
    /// a row, so the baseline always reflects the last response that actually contributed bytes.
    std::optional<String> last_delivering_response_etag;

    /// ETag of the response `impl` currently represents, and whether that response has delivered a byte
    /// yet. Both are set together in initialize(); nextImpl() flips `pending_response_bytes_delivered`
    /// to true (and advances last_delivering_response_etag) the moment this response's first byte
    /// reaches the consumer.
    String pending_response_etag;
    bool pending_response_bytes_delivered = false;

    bool response_identity_changed = false;

    ReadSettings read_settings;

    bool use_external_buffer;

    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;

    bool read_all_range_successfully = false;

    const S3CredentialsRefreshCallback credentials_refresh_callback;

    mutable BlobStorageLogWriterPtr blob_storage_log;
};

}

#endif
