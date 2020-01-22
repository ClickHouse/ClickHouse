#include <Common/config.h>

#if USE_AWS_S3

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromIStream.h>

#include <common/logger_useful.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/S3Client.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    size_t buffer_size_
)
    : SeekableReadBuffer(nullptr, 0)
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , buffer_size(buffer_size_)
{
}

bool ReadBufferFromS3::nextImpl()
{
    if (!initialized)
    {
        impl = initialize();
        initialized = true;
    }

    if (!impl->next())
        return false;
    internal_buffer = impl->buffer();
    working_buffer = internal_buffer;
    return true;
}

off_t ReadBufferFromS3::doSeek(off_t offset_, int) {
    if (!initialized && offset_)
        offset = offset_;

    return offset;
}

std::unique_ptr<ReadBuffer> ReadBufferFromS3::initialize() {
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    if (offset != 0)
        req.SetRange(std::to_string(offset) + "-");

    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        read_result = outcome.GetResultWithOwnership();
        return std::make_unique<ReadBufferFromIStream>(read_result.GetBody(), buffer_size);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

}

#endif
