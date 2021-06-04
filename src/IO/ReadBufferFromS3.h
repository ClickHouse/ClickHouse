#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#    include <memory>

#    include <IO/HTTPCommon.h>
#    include <IO/ReadBuffer.h>
#    include <aws/s3/model/GetObjectResult.h>
#    include "SeekableReadBuffer.h"

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
    UInt64 s3_max_single_read_retries;
    size_t buffer_size;
    off_t offset = 0;
    Aws::S3::Model::GetObjectResult read_result;
    std::unique_ptr<ReadBuffer> impl;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromS3");

public:
    explicit ReadBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        UInt64 s3_max_single_read_retries_,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

private:
    std::unique_ptr<ReadBuffer> initialize();
};

}

#endif
