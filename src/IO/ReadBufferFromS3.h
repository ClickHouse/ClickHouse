#pragma once

#include <Common/RangeGenerator.h>
#include <Common/config.h>

#if USE_AWS_S3

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileName.h>

#include <aws/s3/model/GetObjectResult.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
/**
 * Perform S3 HTTP GET request and provide response to read.
 */
class ReadBufferFromS3 : public SeekableReadBuffer, public WithFileName, public WithFileSize
{
private:
    std::shared_ptr<const Aws::S3::S3Client> client_ptr;
    String bucket;
    String key;
    String version_id;
    UInt64 max_single_read_retries;

    /// These variables are atomic because they can be used for `logging only`
    /// (where it is not important to get consistent result)
    /// from separate thread other than the one which uses the buffer for s3 reading.
    std::atomic<off_t> offset = 0;
    std::atomic<off_t> read_until_position = 0;

    Aws::S3::Model::GetObjectResult read_result;
    std::unique_ptr<ReadBuffer> impl;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromS3");

public:
    ReadBufferFromS3(
        std::shared_ptr<const Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const String & version_id_,
        UInt64 max_single_read_retries_,
        const ReadSettings & settings_,
        bool use_external_buffer = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> getFileSize() override;

    void setReadUntilPosition(size_t position) override;

    Range getRemainingReadRange() const override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    String getFileName() const override { return bucket + "/" + key; }

private:
    std::unique_ptr<ReadBuffer> initialize();

    ReadSettings read_settings;

    bool use_external_buffer;

    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;

    std::optional<size_t> file_size;
};

/// Creates separate ReadBufferFromS3 for sequence of ranges of particular object
class ReadBufferS3Factory : public ParallelReadBuffer::ReadBufferFactory, public WithFileName
{
public:
    explicit ReadBufferS3Factory(
        std::shared_ptr<const Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const String & version_id_,
        size_t range_step_,
        size_t object_size_,
        UInt64 s3_max_single_read_retries_,
        const ReadSettings & read_settings_)
        : client_ptr(client_ptr_)
        , bucket(bucket_)
        , key(key_)
        , version_id(version_id_)
        , read_settings(read_settings_)
        , range_generator(object_size_, range_step_)
        , range_step(range_step_)
        , object_size(object_size_)
        , s3_max_single_read_retries(s3_max_single_read_retries_)
    {
        assert(range_step > 0);
        assert(range_step < object_size);
    }

    SeekableReadBufferPtr getReader() override;

    off_t seek(off_t off, [[maybe_unused]] int whence) override;

    std::optional<size_t> getFileSize() override;

    String getFileName() const override { return bucket + "/" + key; }

private:
    std::shared_ptr<const Aws::S3::S3Client> client_ptr;
    const String bucket;
    const String key;
    const String version_id;
    ReadSettings read_settings;

    RangeGenerator range_generator;
    size_t range_step;
    size_t object_size;

    UInt64 s3_max_single_read_retries;
};

}

#endif
