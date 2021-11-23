#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>

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
class ReadBufferFromS3 : public SeekableReadBuffer
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
        size_t read_until_position_ = 0);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

private:
    std::unique_ptr<ReadBuffer> initialize();

    ReadSettings read_settings;
    bool use_external_buffer;
    off_t read_until_position = 0;
};

}

#endif
