#pragma once

#include <IO/S3Settings.h>
#include "config.h"

#if USE_AWS_S3

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WithFileName.h>

#include <aws/s3/model/GetObjectResult.h>

namespace DB
{
/**
 * Perform S3 HTTP GET request and provide response to read.
 */
class ReadBufferFromS3 : public ReadBufferFromFileBase
{
private:
    std::shared_ptr<const S3::Client> client_ptr;
    String bucket;
    String key;
    String version_id;
    const S3::S3RequestSettings request_settings;

    /// These variables are atomic because they can be used for `logging only`
    /// (where it is not important to get consistent result)
    /// from separate thread other than the one which uses the buffer for s3 reading.
    std::atomic<off_t> offset = 0;
    std::atomic<off_t> read_until_position = 0;

    std::optional<Aws::S3::Model::GetObjectResult> read_result;
    std::unique_ptr<ReadBuffer> impl;

    LoggerPtr log = getLogger("ReadBufferFromS3");

public:
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
        std::optional<size_t> file_size = std::nullopt);

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

private:
    std::unique_ptr<ReadBuffer> initialize(size_t attempt);

    /// If true, if we destroy impl now, no work was wasted. Just for metrics.
    bool atEndOfRequestedRangeGuess();

    /// Call inside catch() block if GetObject fails. Bumps metrics, logs the error.
    /// Returns true if the error looks retriable.
    bool processException(size_t read_offset, size_t attempt) const;

    Aws::S3::Model::GetObjectResult sendRequest(size_t attempt, size_t range_begin, std::optional<size_t> range_end_incl) const;

    ReadSettings read_settings;

    bool use_external_buffer;

    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;

    bool read_all_range_successfully = false;
};

}

#endif
