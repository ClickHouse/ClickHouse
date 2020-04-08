#include <Common/config.h>

#if USE_AWS_S3

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromIStream.h>

#include <common/logger_useful.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/S3Client.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}


ReadBufferFromS3::ReadBufferFromS3(const std::shared_ptr<Aws::S3::S3Client> & client_ptr,
        const String & bucket,
        const String & key,
        size_t buffer_size_): ReadBuffer(nullptr, 0)
{
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        read_result = outcome.GetResultWithOwnership();
        impl = std::make_unique<ReadBufferFromIStream>(read_result.GetBody(), buffer_size_);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

bool ReadBufferFromS3::nextImpl()
{
    if (!impl->next())
        return false;
    internal_buffer = impl->buffer();
    working_buffer = internal_buffer;
    return true;
}

}

#endif
