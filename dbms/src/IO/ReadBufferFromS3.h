#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <aws/s3/model/GetObjectResult.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{
/** Perform S3 HTTP GET request and provide response to read.
  */
class ReadBufferFromS3 : public ReadBuffer
{
private:
    Logger * log = &Logger::get("ReadBufferFromS3");
    Aws::S3::Model::GetObjectResult read_result;

protected:
    std::unique_ptr<ReadBuffer> impl;

public:
    explicit ReadBufferFromS3(const std::shared_ptr<Aws::S3::S3Client> & client_ptr,
        const String & bucket,
        const String & key,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;
};

}

#endif
