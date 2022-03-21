#pragma once

#include <Common/RangeGenerator.h>
#include <Common/config.h>

#if USE_AWS_S3

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ParallelReadBuffer.h>

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
class ReadBufferFromS3 : public SeekableReadBufferWithSize
{
private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    String key;
    UInt64 max_single_read_retries;
    off_t offset = 0;

    Aws::S3::Model::GetObjectResult read_result;
    std::unique_ptr<ReadBuffer> impl;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromS3");

public:
    ReadBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        UInt64 max_single_read_retries_,
        const ReadSettings & settings_,
        bool use_external_buffer = false,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> getTotalSize() override;

    void setReadUntilPosition(size_t position) override;

    Range getRemainingReadRange() const override { return Range{ .left = static_cast<size_t>(offset), .right = read_until_position }; }

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    void setRange(off_t begin, off_t end)
    {
        offset = begin;
        read_until_position = end;
    }

private:
    std::unique_ptr<ReadBuffer> initialize();

    ReadSettings read_settings;

    bool use_external_buffer;

    off_t read_until_position = 0;

    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;
};

/// Creates separate ReadBufferFromS3 for sequence of ranges of particular object
class ReadBufferS3Factory : public ParallelReadBuffer::ReadBufferFactory
{
public:
    explicit ReadBufferS3Factory(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t range_step_,
        size_t object_size_,
        UInt64 s3_max_single_read_retries_,
        const ReadSettings &read_settings)
        : client_ptr(client_ptr_)
        , bucket(bucket_)
        , key(key_)
        , settings(read_settings)
        , range_generator(object_size_, range_step_)
        , range_step(range_step_)
        , object_size(object_size_)
        , s3_max_single_read_retries(s3_max_single_read_retries_)
    {
        assert(range_step > 0);
        assert(range_step < object_size);
    }

    size_t totalRanges() const
    {
        return static_cast<size_t>(round(static_cast<float>(object_size) / range_step));
    }

    SeekableReadBufferPtr getReader() override
    {
        const auto next_range = range_generator.nextRange();
        if (!next_range)
        {
            return nullptr;
        }

        auto reader = std::make_shared<ReadBufferFromS3>(
                client_ptr, bucket, key, s3_max_single_read_retries, settings);
        reader->setRange(next_range->first, next_range->second - 1);
        return reader;
    }

    off_t seek(off_t off, [[maybe_unused]] int whence) override
    {
        range_generator = RangeGenerator{object_size, range_step, static_cast<size_t>(off)};
        return off;
    }

    std::optional<size_t> getTotalSize() override { return object_size; }

private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    const String bucket;
    const String key;
    ReadSettings settings;

    RangeGenerator range_generator;
    size_t range_step;
    size_t object_size;

    UInt64 s3_max_single_read_retries;
};

}

#endif
