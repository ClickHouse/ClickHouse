#include <Common/config.h>

#if USE_AWS_S3

#include <Common/logger_useful.h>
#include <Common/Throttler.h>

#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <utility>


namespace ProfileEvents
{
    extern const Event WriteBufferFromS3Bytes;
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

struct WriteBufferFromS3::UploadPartTask
{
    Aws::S3::Model::UploadPartRequest req;
    bool is_finised = false;
    std::string tag;
    std::exception_ptr exception;
};

struct WriteBufferFromS3::PutObjectTask
{
    Aws::S3::Model::PutObjectRequest req;
    bool is_finised = false;
    std::exception_ptr exception;
};

WriteBufferFromS3::WriteBufferFromS3(
    std::shared_ptr<const Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    const S3Settings::ReadWriteSettings & s3_settings_,
    std::optional<std::map<String, String>> object_metadata_,
    size_t buffer_size_,
    ScheduleFunc schedule_,
    const WriteSettings & write_settings_)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size_, nullptr, 0)
    , bucket(bucket_)
    , key(key_)
    , client_ptr(std::move(client_ptr_))
    , upload_part_size(s3_settings_.min_upload_part_size)
    , s3_settings(s3_settings_)
    , object_metadata(std::move(object_metadata_))
    , schedule(std::move(schedule_))
    , write_settings(write_settings_)
{
    allocateBuffer();
}

void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    /// Buffer in a bad state after exception
    if (temporary_buffer->tellp() == -1)
        allocateBuffer();

    size_t size = offset();
    temporary_buffer->write(working_buffer.begin(), size);

    ThreadGroupStatusPtr running_group = CurrentThread::isInitialized() && CurrentThread::get().getThreadGroup()
            ? CurrentThread::get().getThreadGroup()
            : MainThreadStatus::getInstance().getThreadGroup();

    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Bytes, offset());
    last_part_size += offset();
    if (write_settings.remote_throttler)
        write_settings.remote_throttler->add(offset());

    /// Data size exceeds singlepart upload threshold, need to use multipart upload.
    if (multipart_upload_id.empty() && last_part_size > s3_settings.max_single_part_upload_size)
        createMultipartUpload();

    if (!multipart_upload_id.empty() && last_part_size > upload_part_size)
    {
        writePart();

        allocateBuffer();
    }

    waitForReadyBackGroundTasks();
}

void WriteBufferFromS3::allocateBuffer()
{
    if (total_parts_uploaded != 0 && total_parts_uploaded % s3_settings.upload_part_size_multiply_parts_count_threshold == 0)
        upload_part_size *= s3_settings.upload_part_size_multiply_factor;

    temporary_buffer = Aws::MakeShared<Aws::StringStream>("temporary buffer");
    temporary_buffer->exceptions(std::ios::badbit);
    last_part_size = 0;
}

WriteBufferFromS3::~WriteBufferFromS3()
{
#ifndef NDEBUG
    if (!finalized)
    {
        LOG_ERROR(log, "WriteBufferFromS3 is not finalized in destructor. It's a bug");
        std::terminate();
    }
#else
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
#endif
}

void WriteBufferFromS3::preFinalize()
{
    next();

    if (multipart_upload_id.empty())
    {
        makeSinglepartUpload();
    }
    else
    {
        /// Write rest of the data as last part.
        writePart();
    }

    is_prefinalized = true;
}

void WriteBufferFromS3::finalizeImpl()
{
    if (!is_prefinalized)
        preFinalize();

    waitForAllBackGroundTasks();

    if (!multipart_upload_id.empty())
        completeMultipartUpload();
}

void WriteBufferFromS3::createMultipartUpload()
{
    Aws::S3::Model::CreateMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());

    auto outcome = client_ptr->CreateMultipartUpload(req);

    if (outcome.IsSuccess())
    {
        multipart_upload_id = outcome.GetResult().GetUploadId();
        LOG_TRACE(log, "Multipart upload has created. Bucket: {}, Key: {}, Upload id: {}", bucket, key, multipart_upload_id);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

void WriteBufferFromS3::writePart()
{
    auto size = temporary_buffer->tellp();

    LOG_TRACE(log, "Writing part. Bucket: {}, Key: {}, Upload_id: {}, Size: {}", bucket, key, multipart_upload_id, size);

    if (size < 0)
    {
        LOG_WARNING(log, "Skipping part upload. Buffer is in bad state, it means that we have tried to upload something, but got an exception.");
        return;
    }

    if (size == 0)
    {
        LOG_TRACE(log, "Skipping writing part. Buffer is empty.");
        return;
    }

    if (part_tags.size() == S3_WARN_MAX_PARTS)
    {
        // Don't throw exception here by ourselves but leave the decision to take by S3 server.
        LOG_WARNING(log, "Maximum part number in S3 protocol has reached (too many parts). Server may not accept this whole upload.");
    }

    if (schedule)
    {
        UploadPartTask * task = nullptr;
        int part_number;
        {
            std::lock_guard lock(bg_tasks_mutex);
            task = &upload_object_tasks.emplace_back();
            ++num_added_bg_tasks;
            part_number = num_added_bg_tasks;
        }

        fillUploadRequest(task->req, part_number);

        schedule([this, task]()
        {
            try
            {
                processUploadRequest(*task);
            }
            catch (...)
            {
                task->exception = std::current_exception();
            }

            {
                std::lock_guard lock(bg_tasks_mutex);
                task->is_finised = true;
                ++num_finished_bg_tasks;

                /// Notification under mutex is important here.
                /// Otherwise, WriteBuffer could be destroyed in between
                /// Releasing lock and condvar notification.
                bg_tasks_condvar.notify_one();
            }
        });
    }
    else
    {
        UploadPartTask task;
        fillUploadRequest(task.req, part_tags.size() + 1);
        processUploadRequest(task);
        part_tags.push_back(task.tag);
    }
}

void WriteBufferFromS3::fillUploadRequest(Aws::S3::Model::UploadPartRequest & req, int part_number)
{
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetPartNumber(part_number);
    req.SetUploadId(multipart_upload_id);
    req.SetContentLength(temporary_buffer->tellp());
    req.SetBody(temporary_buffer);

    /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");
}

void WriteBufferFromS3::processUploadRequest(UploadPartTask & task)
{
    auto outcome = client_ptr->UploadPart(task.req);

    if (outcome.IsSuccess())
    {
        task.tag = outcome.GetResult().GetETag();
        LOG_TRACE(log, "Writing part finished. Bucket: {}, Key: {}, Upload_id: {}, Etag: {}, Parts: {}", bucket, key, multipart_upload_id, task.tag, part_tags.size());
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);

    total_parts_uploaded++;
}

void WriteBufferFromS3::completeMultipartUpload()
{
    LOG_TRACE(log, "Completing multipart upload. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", bucket, key, multipart_upload_id, part_tags.size());

    if (part_tags.empty())
        throw Exception("Failed to complete multipart upload. No parts have uploaded", ErrorCodes::S3_ERROR);

    Aws::S3::Model::CompleteMultipartUploadRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetUploadId(multipart_upload_id);

    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (size_t i = 0; i < part_tags.size(); ++i)
    {
        Aws::S3::Model::CompletedPart part;
        multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(i + 1));
    }

    req.SetMultipartUpload(multipart_upload);

    auto outcome = client_ptr->CompleteMultipartUpload(req);

    if (outcome.IsSuccess())
        LOG_TRACE(log, "Multipart upload has completed. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", bucket, key, multipart_upload_id, part_tags.size());
    else
    {
        throw Exception(ErrorCodes::S3_ERROR, "{} Tags:{}",
            outcome.GetError().GetMessage(),
            fmt::join(part_tags.begin(), part_tags.end(), " "));
    }
}

void WriteBufferFromS3::makeSinglepartUpload()
{
    auto size = temporary_buffer->tellp();
    bool with_pool = static_cast<bool>(schedule);

    LOG_TRACE(log, "Making single part upload. Bucket: {}, Key: {}, Size: {}, WithPool: {}", bucket, key, size, with_pool);

    if (size < 0)
    {
        LOG_WARNING(log, "Skipping single part upload. Buffer is in bad state, it mean that we have tried to upload something, but got an exception.");
        return;
    }

    if (schedule)
    {
        put_object_task = std::make_unique<PutObjectTask>();

        fillPutRequest(put_object_task->req);

        schedule([this]()
        {
            try
            {
                processPutRequest(*put_object_task);
            }
            catch (...)
            {
                put_object_task->exception = std::current_exception();
            }

            {
                std::lock_guard lock(bg_tasks_mutex);
                put_object_task->is_finised = true;

                /// Notification under mutex is important here.
                /// Othervies, WriteBuffer could be destroyed in between
                /// Releasing lock and condvar notification.
                bg_tasks_condvar.notify_one();
            }
        });
    }
    else
    {
        PutObjectTask task;
        fillPutRequest(task.req);
        processPutRequest(task);
    }
}

void WriteBufferFromS3::fillPutRequest(Aws::S3::Model::PutObjectRequest & req)
{
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetContentLength(temporary_buffer->tellp());
    req.SetBody(temporary_buffer);
    if (object_metadata.has_value())
        req.SetMetadata(object_metadata.value());

    /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");
}

void WriteBufferFromS3::processPutRequest(PutObjectTask & task)
{
    auto outcome = client_ptr->PutObject(task.req);
    bool with_pool = static_cast<bool>(schedule);
    if (outcome.IsSuccess())
        LOG_TRACE(log, "Single part upload has completed. Bucket: {}, Key: {}, Object size: {}, WithPool: {}", bucket, key, task.req.GetContentLength(), with_pool);
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

void WriteBufferFromS3::waitForReadyBackGroundTasks()
{
    if (schedule)
    {
        std::lock_guard lock(bg_tasks_mutex);
        {
            while (!upload_object_tasks.empty() && upload_object_tasks.front().is_finised)
            {
                auto & task = upload_object_tasks.front();
                auto exception = task.exception;
                auto tag = std::move(task.tag);
                upload_object_tasks.pop_front();

                if (exception)
                {
                    waitForAllBackGroundTasks();
                    std::rethrow_exception(exception);
                }

                part_tags.push_back(tag);
            }
        }
    }
}

void WriteBufferFromS3::waitForAllBackGroundTasks()
{
    if (schedule)
    {
        std::unique_lock lock(bg_tasks_mutex);
        bg_tasks_condvar.wait(lock, [this]() { return num_added_bg_tasks == num_finished_bg_tasks; });

        while (!upload_object_tasks.empty())
        {
            auto & task = upload_object_tasks.front();
            if (task.exception)
                std::rethrow_exception(task.exception);

            part_tags.push_back(task.tag);

            upload_object_tasks.pop_front();
        }

        if (put_object_task)
        {
            bg_tasks_condvar.wait(lock, [this]() { return put_object_task->is_finised; });
            if (put_object_task->exception)
                std::rethrow_exception(put_object_task->exception);
        }
    }
}

}

#endif
