#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/WriteBufferFromS3.h>
#    include <IO/WriteHelpers.h>

#    include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#    include <aws/core/utils/memory/stl/AWSStringStream.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/CompleteMultipartUploadRequest.h>
#    include <aws/s3/model/AbortMultipartUploadRequest.h>
#    include <aws/s3/model/CreateMultipartUploadRequest.h>
#    include <aws/s3/model/PutObjectRequest.h>
#    include <aws/s3/model/UploadPartRequest.h>
#    include <common/logger_useful.h>

#    include <utility>


namespace ProfileEvents
{
    extern const Event S3WriteBytes;
}

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
    bool is_multipart_,
    size_t buffer_size_)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size_, nullptr, 0)
    , is_multipart(is_multipart_)
    , bucket(bucket_)
    , key(key_)
    , client_ptr(std::move(client_ptr_))
    , minimum_upload_part_size{minimum_upload_part_size_}
    , temporary_buffer{std::make_unique<WriteBufferFromOwnString>()}
    , last_part_size{0}
{
    if (is_multipart)
        initiate();
}

void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    temporary_buffer->write(working_buffer.begin(), offset());

    ProfileEvents::increment(ProfileEvents::S3WriteBytes, offset());

    if (is_multipart)
    {
        last_part_size += offset();

        if (last_part_size > minimum_upload_part_size)
        {
            writePart(temporary_buffer->str());
            last_part_size = 0;
            temporary_buffer->restart();
        }
    }
}

void WriteBufferFromS3::finalize()
{
    finalizeImpl();
}

void WriteBufferFromS3::finalizeImpl()
{
    if (!finalized)
    {
        next();

        if (is_multipart)
            writePart(temporary_buffer->str());

        complete();

        finalized = true;
    }
}

WriteBufferFromS3::~WriteBufferFromS3()
{
    try
    {
        finalizeImpl();
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
        LOG_DEBUG(log, "Multipart upload initiated. Upload id: {}", upload_id);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}


void WriteBufferFromS3::writePart(const String & data)
{
    if (data.empty())
        return;

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

    LOG_TRACE(log, "Writing part. Bucket: {}, Key: {}, Upload_id: {}, Data size: {}", bucket, key, upload_id, data.size());

    if (outcome.IsSuccess())
    {
        auto etag = outcome.GetResult().GetETag();
        part_tags.push_back(etag);
        LOG_DEBUG(log, "Writing part finished. Total parts: {}, Upload_id: {}, Etag: {}", part_tags.size(), upload_id, etag);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}


void WriteBufferFromS3::complete()
{
    if (is_multipart)
    {
        if (part_tags.empty())
        {
            LOG_DEBUG(log, "Multipart upload has no data. Aborting it. Bucket: {}, Key: {}, Upload_id: {}", bucket, key, upload_id);

            Aws::S3::Model::AbortMultipartUploadRequest req;
            req.SetBucket(bucket);
            req.SetKey(key);
            req.SetUploadId(upload_id);

            auto outcome = client_ptr->AbortMultipartUpload(req);

            if (outcome.IsSuccess())
                LOG_DEBUG(log, "Aborting multipart upload completed. Upload_id: {}", upload_id);
            else
                throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);

            return;
        }

        LOG_DEBUG(log, "Completing multipart upload. Bucket: {}, Key: {}, Upload_id: {}", bucket, key, upload_id);

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
            LOG_DEBUG(log, "Multipart upload completed. Upload_id: {}", upload_id);
        else
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
    else
    {
        LOG_DEBUG(log, "Making single part upload. Bucket: {}, Key: {}", bucket, key);

        Aws::S3::Model::PutObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);

        /// This could be improved using an adapter to WriteBuffer.
        const std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::StringStream>("temporary buffer", temporary_buffer->str());
        temporary_buffer = std::make_unique<WriteBufferFromOwnString>();
        req.SetBody(input_data);

        auto outcome = client_ptr->PutObject(req);

        if (outcome.IsSuccess())
            LOG_DEBUG(log, "Single part upload has completed. Bucket: {}, Key: {}", bucket, key);
        else
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

}

#endif
