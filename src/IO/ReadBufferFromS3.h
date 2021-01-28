#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#    include <memory>

#    include <IO/HTTPCommon.h>
#    include <IO/ReadBuffer.h>
#    include <aws/s3/model/GetObjectResult.h>
#    include "ReadBufferFanIn.h"
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
    size_t buffer_size;
    bool initialized = false;

    off_t offset = 0;
    off_t read_end = 0;

    Aws::S3::Model::GetObjectResult read_result;
    std::unique_ptr<ReadBuffer> impl;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromS3");

public:
    explicit ReadBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    void setRange(size_t begin, size_t end);

private:
    void initialize();
    std::unique_ptr<ReadBuffer> createImpl();
    void checkNotInitialized() const;
};


/// Creates separate ReadBufferFromS3 for sequence of ranges of particular object
class ReadBufferS3Factory : public ReadBufferFanIn::ReadBufferFactory
{
public:
    explicit ReadBufferS3Factory(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t range_step_,
        size_t object_size_)
        : client_ptr(client_ptr_)
        , bucket(bucket_)
        , key(key_)
        , from_range(0)
        , range_step(range_step_)
        , object_size(object_size_)
    {
        assert(range_step > 0);
        assert(range_step < object_size);
    }

    virtual ReadBufferPtr getReader() override;

private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    const String bucket;
    const String key;

    size_t from_range;
    size_t range_step;
    size_t object_size;

};

}

#endif
