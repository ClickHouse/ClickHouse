#include <Common/config.h>

#if USE_AWS_S3

#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <common/logger_useful.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>

#include <utility>


namespace DB
{

// S3 protocol does not allow to have multipart upload with more than 10000 parts.
// In case server does not return an error on exceeding that number, we print a warning
// because custom S3 implementation may allow relaxed requirements on that.
const int S3_WARN_MAX_PARTS = 10000;


namespace ErrorCodes
{
    extern const int S3_ERROR;
}


WriteBufferFromS3::WriteBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    size_t minimum_upload_part_size_,
    size_t buffer_size_
)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size_, nullptr, 0)
    , bucket(bucket_)
    , key(key_)
    , client_ptr(std::move(client_ptr_))
    , minimum_upload_part_size {minimum_upload_part_size_}
    , temporary_buffer {std::make_unique<WriteBufferFromString>(buffer_string)}
    , last_part_size {0}
{
    initiate();
}


void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    temporary_buffer->write(working_buffer.begin(), offset());

    last_part_size += offset();

    if (last_part_size > minimum_upload_part_size)
    {
        temporary_buffer->finish();
        writePart(buffer_string);
        last_part_size = 0;
        temporary_buffer = std::make_unique<WriteBufferFromString>(buffer_string);
    }
}


void WriteBufferFromS3::finalize()
{
    temporary_buffer->finish();
    if (!buffer_string.empty())
    {
        writePart(buffer_string);
    }

    complete();
}


WriteBufferFromS3::~WriteBufferFromS3()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void WriteBufferFromS3::initiate()
{
    Aws::S3::Model::CreateMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    auto outcome = client_ptr->CreateMultipartUpload(req);

    if (outcome.IsSuccess())
    {
        upload_id = outcome.GetResult().GetUploadId();
        LOG_DEBUG(log, "Multipart upload initiated. Upload id = " + upload_id);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}


void WriteBufferFromS3::writePart(const String & data)
{
    if (part_tags.size() == S3_WARN_MAX_PARTS)
    {
        // Don't throw exception here by ourselves but leave the decision to take by S3 server.
        LOG_WARNING(log, "Maximum part number in S3 protocol has reached (too many parts). Server may not accept this whole upload.");
    }

    Aws::S3::Model::UploadPartRequest req;

    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetPartNumber(part_tags.size() + 1);
    req.SetUploadId(upload_id);
    req.SetContentLength(data.size());
    req.SetBody(std::make_shared<Aws::StringStream>(data));

    auto outcome = client_ptr->UploadPart(req);

    if (outcome.IsSuccess())
    {
        auto etag = outcome.GetResult().GetETag();
        part_tags.push_back(etag);
        LOG_DEBUG(log, "Write part " + std::to_string(part_tags.size()) + " finished. Upload id = " + upload_id + ". Etag = " + etag);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}


void WriteBufferFromS3::complete()
{
    Aws::S3::Model::CompleteMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(upload_id);

    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (size_t i = 0; i < part_tags.size(); ++i)
    {
        Aws::S3::Model::CompletedPart part;
        multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(i + 1));
    }

    req.SetMultipartUpload(multipart_upload);

    auto outcome = client_ptr->CompleteMultipartUpload(req);

    if (outcome.IsSuccess())
        LOG_DEBUG(log, "Multipart upload completed. Upload_id = " + upload_id);
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

}

#endif
