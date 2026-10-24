#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <memory>
#include <base/types.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/client.h>

namespace DB
{

class BlobStorageLogWriter;
using BlobStorageLogWriterPtr = std::shared_ptr<BlobStorageLogWriter>;

/// Reads a GCS object through the native google-cloud-cpp storage client.
///
/// Backed by a `google::cloud::storage::ObjectReadStream` (a std::istream). Ranged reads are used
/// for seeks and for a bounded right edge (`read_until_position`), mirroring ReadBufferFromS3 /
/// ReadBufferFromAzureBlobStorage semantics.
///
/// When `expected_generation` is set, every `ReadObject` request carries an `IfGenerationMatch`
/// precondition, so a concurrent in-place overwrite fails the read instead of splicing bytes from
/// two object generations (the native counterpart of `s3_validate_etag_on_read`).
///
/// Unlike S3/Azure, where one `initialize()` call is one discrete network request with a known
/// content length, a GCS `ObjectReadStream` streams bytes across many `nextImpl` calls with no
/// natural per-request boundary. So a successful read is logged to `system.blob_storage_log` (if
/// enabled) as a single aggregate event in the destructor (mirroring ReadBufferFromHDFS), while an
/// open or mid-stream failure is logged immediately, at the point it is detected.
class ReadBufferFromGCS : public ReadBufferFromFileBase
{
public:
    ReadBufferFromGCS(
        std::shared_ptr<google::cloud::storage::Client> client_,
        const String & bucket_,
        const String & key_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false,
        std::optional<size_t> file_size_ = std::nullopt,
        std::optional<Int64> expected_generation_ = std::nullopt,
        BlobStorageLogWriterPtr blob_storage_log_ = {},
        bool for_disk_ = false);

    ~ReadBufferFromGCS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> tryGetFileSize() override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    bool supportsRightBoundedReads() const override { return true; }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    /// Enables `ParallelReadBuffer` to split one large object across `max_download_threads` ranged
    /// reads, which is what reading a whole object -- the `gcs` table function, or a Parquet or ORC
    /// file -- goes through. Without it such a read is served by a single stream.
    bool supportsReadAt() override { return true; }

    String getFileName() const override { return bucket + "/" + key; }

private:
    void initialize();

    std::shared_ptr<google::cloud::storage::Client> client;
    const String bucket;
    const String key;

    ReadSettings read_settings;
    bool use_external_buffer;
    bool restricted_seek;

    /// File offset of the byte just past the current working buffer.
    off_t offset = 0;
    /// Right boundary (exclusive); 0 means "read to the end of the object".
    off_t read_until_position = 0;

    /// If set, pin every read request to this object generation (see the class comment).
    std::optional<Int64> expected_generation;
    /// Attributes request counters to `DiskGCS*` in addition to `GCS*`.
    bool for_disk;

    BlobStorageLogWriterPtr blob_storage_log;
    size_t total_bytes_read = 0;
    size_t total_read_microseconds = 0;
    bool read_attempted = false;
    bool read_failed = false;

    bool initialized = false;
    std::unique_ptr<google::cloud::storage::ObjectReadStream> read_stream;

    /// Own buffer used when not reading into an externally provided buffer.
    std::vector<char> tmp_buffer;
    size_t tmp_buffer_size;
    char * data_ptr = nullptr;
    size_t data_capacity = 0;

    LoggerPtr log = getLogger("ReadBufferFromGCS");
};

}

#endif
