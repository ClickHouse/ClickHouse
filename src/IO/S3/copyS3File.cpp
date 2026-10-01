#include <IO/S3/copyS3File.h>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Common/typeid_cast.h>
#include <IO/S3RequestSettings.h>
#include <Common/BlobStorageLogWriter.h>
#include <Interpreters/Context.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdStreamFromReadBuffer.h>
#include <IO/ReadBufferFromS3.h>

#include <IO/S3/Requests.h>

#include <fmt/ranges.h>


namespace ProfileEvents
{
    extern const Event WriteBufferFromS3Bytes;
    extern const Event WriteBufferFromS3Microseconds;
    extern const Event WriteBufferFromS3RequestsErrors;

    extern const Event S3CreateMultipartUpload;
    extern const Event S3CompleteMultipartUpload;
    extern const Event S3PutObject;
    extern const Event S3CopyObject;
    extern const Event S3UploadPart;
    extern const Event S3UploadPartCopy;

    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
    extern const Event DiskS3PutObject;
    extern const Event DiskS3CopyObject;
    extern const Event DiskS3UploadPart;
    extern const Event DiskS3UploadPartCopy;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool allow_native_copy;
    extern const S3RequestSettingsBool check_objects_after_upload;
    extern const S3RequestSettingsUInt64 max_part_number;
    extern const S3RequestSettingsBool allow_multipart_copy;
    extern const S3RequestSettingsUInt64 max_single_operation_copy_size;
    extern const S3RequestSettingsUInt64 max_single_part_upload_size;
    extern const S3RequestSettingsUInt64 max_unexpected_write_error_retries;
    extern const S3RequestSettingsUInt64 max_upload_part_size;
    extern const S3RequestSettingsUInt64 min_upload_part_size;
    extern const S3RequestSettingsString storage_class_name;
    extern const S3RequestSettingsUInt64 max_inflight_parts_for_one_file;
}

namespace
{
    class UploadHelper
    {
    public:
        UploadHelper(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            const S3::S3RequestSettings & request_settings_,
            const std::optional<std::map<String, String>> & object_metadata_,
            ThreadPoolCallbackRunnerUnsafe<void> schedule_,
            BlobStorageLogWriterPtr blob_storage_log_,
            const LoggerPtr log_)
            : client_ptr(client_ptr_)
            , dest_bucket(dest_bucket_)
            , dest_key(dest_key_)
            , request_settings(request_settings_)
            , object_metadata(object_metadata_)
            , schedule(schedule_)
            , blob_storage_log(blob_storage_log_)
            , log(log_)
            , num_parts(0)
            , normal_part_size(0)
        {
        }

        virtual ~UploadHelper() = default;

    protected:
        std::shared_ptr<const S3::Client> client_ptr;
        const String & dest_bucket;
        const String & dest_key;
        const S3::S3RequestSettings & request_settings;
        const std::optional<std::map<String, String>> & object_metadata;
        ThreadPoolCallbackRunnerUnsafe<void> schedule;
        BlobStorageLogWriterPtr blob_storage_log;
        const LoggerPtr log;

        /// Represents a task uploading a single part.
        /// Keep this struct small because there can be thousands of parts.
        /// For example, `UploadPartTask` must not contain a read buffer or `S3::UploadPartRequest`
        /// because such read buffer can consume about 1MB memory and it could cause memory issues when the number of parts is big enough.
        struct UploadPartTask
        {
            size_t part_number;
            size_t part_offset;
            size_t part_size;
        };

        size_t num_parts;
        size_t normal_part_size;
        String multipart_upload_id;
        std::deque<String> multipart_tags;
        std::atomic<size_t> num_finished_parts = 0;
        std::atomic<bool> has_failed = false;

        void fillCreateMultipartRequest(S3::CreateMultipartUploadRequest & request)
        {
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request.SetContentType("binary/octet-stream");

            if (object_metadata.has_value())
                request.SetMetadata(object_metadata.value());

            const auto & storage_class_name = request_settings[S3RequestSetting::storage_class_name];
            if (!storage_class_name.value.empty())
                request.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            client_ptr->setKMSHeaders(request);
        }

        void createMultipartUpload()
        {
            S3::CreateMultipartUploadRequest request;
            fillCreateMultipartRequest(request);

            ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
            if (client_ptr->isClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);

            Stopwatch watch;
            auto outcome = client_ptr->CreateMultipartUpload(request);
            auto elapsed = watch.elapsedMicroseconds();

            if (blob_storage_log)
                blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadCreate,
                                           dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0, elapsed,
                                           outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                           outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

            if (!outcome.IsSuccess())
            {
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
            }
            multipart_upload_id = outcome.GetResult().GetUploadId();
            if (multipart_upload_id.empty())
            {
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw Exception(ErrorCodes::S3_ERROR, "Invalid CreateMultipartUpload result: missing UploadId.");
            }
            LOG_TRACE(log, "Multipart upload was created. Bucket: {}, Key: {}, Upload id: {}", dest_bucket, dest_key, multipart_upload_id);
        }

        void completeMultipartUpload()
        {
            LOG_TRACE(log, "Completing multipart upload. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", dest_bucket, dest_key, multipart_upload_id, multipart_tags.size());

            if (multipart_tags.empty())
                throw Exception(ErrorCodes::S3_ERROR, "Failed to complete multipart upload. No parts have uploaded");

            S3::CompleteMultipartUploadRequest request;
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);
            request.SetUploadId(multipart_upload_id);

            Aws::S3::Model::CompletedMultipartUpload multipart_upload;
            for (size_t i = 0; i < multipart_tags.size(); ++i)
            {
                Aws::S3::Model::CompletedPart part;
                multipart_upload.AddParts(part.WithETag(multipart_tags[i]).WithPartNumber(static_cast<int>(i + 1)));
            }

            request.SetMultipartUpload(multipart_upload);

            size_t max_retries = std::max<UInt64>(request_settings[S3RequestSetting::max_unexpected_write_error_retries].value, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
                if (client_ptr->isClientForDisk())
                    ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);

                Stopwatch watch;
                auto outcome = client_ptr->CompleteMultipartUpload(request);
                auto elapsed = watch.elapsedMicroseconds();

                if (blob_storage_log)
                    blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadComplete,
                                               dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0, elapsed,
                                               outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                               outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

                if (outcome.IsSuccess())
                {
                    LOG_TRACE(log, "Multipart upload has completed. Bucket: {}, Key: {}, Upload_id: {}, Parts: {}", dest_bucket, dest_key, multipart_upload_id, multipart_tags.size());
                    break;
                }

                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && (retries < max_retries))
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    /// BTW, NO_SUCH_UPLOAD is expected error and we shouldn't retry it
                    LOG_INFO(log, "Multipart upload failed with NO_SUCH_KEY error for Bucket: {}, Key: {}, Upload_id: {}, Parts: {}, will retry", dest_bucket, dest_key, multipart_upload_id, multipart_tags.size());
                    continue; /// will retry
                }
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Message: {}, Key: {}, Bucket: {}, Tags: {}",
                    outcome.GetError().GetMessage(), dest_key, dest_bucket, fmt::join(multipart_tags.begin(), multipart_tags.end(), " "));
            }
        }

        void abortMultipartUpload()
        {
            LOG_TRACE(log, "Aborting multipart upload. Bucket: {}, Key: {}, Upload_id: {}", dest_bucket, dest_key, multipart_upload_id);
            S3::AbortMultipartUploadRequest abort_request;
            abort_request.SetBucket(dest_bucket);
            abort_request.SetKey(dest_key);
            abort_request.SetUploadId(multipart_upload_id);

            Stopwatch watch;
            auto outcome = client_ptr->AbortMultipartUpload(abort_request);
            auto elapsed = watch.elapsedMicroseconds();

            if (blob_storage_log)
                blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadAbort,
                                           dest_bucket, dest_key, /* local_path_ */ {}, /* data_size */ 0, elapsed,
                                           outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                           outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());
        }

        void checkObjectAfterUpload()
        {
            LOG_TRACE(log, "Checking object {} exists after upload", dest_key);
            S3::checkObjectExists(*client_ptr, dest_bucket, dest_key, {}, "Immediately after upload");
            LOG_TRACE(log, "Object {} exists after upload", dest_key);
        }

        void performMultipartUpload(size_t start_offset, size_t size)
        {
            calculatePartSize(size);
            createMultipartUpload();

            size_t position = start_offset;
            size_t end_position = start_offset + size;

            LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);
            TaskTracker task_tracker(schedule, request_settings[S3RequestSetting::max_inflight_parts_for_one_file], limited_log);

            try
            {
                for (size_t part_number = 1; position < end_position; ++part_number)
                {
                    if (has_failed)
                        break;

                    size_t next_position = std::min(position + normal_part_size, end_position);
                    size_t part_size = next_position - position; /// `part_size` is either `normal_part_size` or smaller if it's the final part.

                    LOG_TRACE(log, "Writing part #{} of {}. Bucket: {}, Key: {}, Upload_id: {}, Size: {}", part_number, num_parts, dest_bucket, dest_key, multipart_upload_id, part_size);

                    assert(part_size);

                    multipart_tags.push_back({});
                    chassert(part_number == multipart_tags.size());
                    auto & part_tag = multipart_tags.back();

                    task_tracker.add([this, part_number, position, part_size, &part_tag]()
                    {
                        UploadPartTask task = {part_number, position, part_size};
                        this->processUploadTask(task, part_tag);
                    });

                    position = next_position;
                }

                task_tracker.waitAll();
                completeMultipartUpload();
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("While performing multipart upload of object {} in bucket {}", dest_key, dest_bucket));

                task_tracker.safeWaitAll();

                try
                {
                    abortMultipartUpload();
                }
                catch (...)
                {
                    tryLogCurrentException(log, fmt::format("While aborting multipart upload of {}", dest_key));
                }

                throw;
            }
        }

        void calculatePartSize(size_t total_size)
        {
            if (!total_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chosen multipart upload for an empty file. This must not happen");

            UInt64 max_part_number = request_settings[S3RequestSetting::max_part_number];
            UInt64 min_upload_part_size = request_settings[S3RequestSetting::min_upload_part_size];
            UInt64 max_upload_part_size = request_settings[S3RequestSetting::max_upload_part_size];

            if (!max_part_number)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_part_number must not be 0");
            if (!min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "min_upload_part_size must not be 0");
            if (max_upload_part_size < min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_upload_part_size must not be less than min_upload_part_size");

            size_t part_size = min_upload_part_size;
            num_parts = (total_size + part_size - 1) / part_size;

            if (num_parts > max_part_number)
            {
                part_size = (total_size + max_part_number - 1) / max_part_number;
                num_parts = (total_size + part_size - 1) / part_size;
            }

            if (part_size > max_upload_part_size)
            {
                part_size = max_upload_part_size;
                num_parts = (total_size + part_size - 1) / part_size;
            }

            String error;
            if (num_parts < 1)
                error = "Number of parts is zero";
            else if (num_parts > max_part_number)
                error = fmt::format("Number of parts exceeds {}/{}", num_parts, max_part_number);
            else if (part_size < min_upload_part_size)
                error = fmt::format("Size of a part is less than {}/{}", part_size, min_upload_part_size);
            else if (part_size > max_upload_part_size)
                error = fmt::format("Size of a part exceeds {}/{}", part_size, max_upload_part_size);

            if (!error.empty())
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "{} while writing {} bytes to S3. Check max_part_number = {}, "
                    "min_upload_part_size = {}, max_upload_part_size = {}",
                    error, total_size, max_part_number, min_upload_part_size, max_upload_part_size);
            }

            /// We've calculated the size of a normal part (the final part can be smaller).
            normal_part_size = part_size;
        }

        void processUploadTask(UploadPartTask & task, String & part_tag)
        {
            if (has_failed)
                return;

            try
            {
                Stopwatch watch;

                auto request = makeUploadPartRequest(task.part_number, task.part_offset, task.part_size);
                auto tag = processUploadPartRequest(*request);

                watch.stop();
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Bytes, task.part_size);
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Microseconds, watch.elapsedMicroseconds());

                part_tag = tag;
                auto finished_count = ++num_finished_parts;

                LOG_TRACE(log, "Finished writing part #{}. Bucket: {}, Key: {}, Upload_id: {}, Etag: {}, Finished parts: {} of {}",
                        task.part_number, dest_bucket, dest_key, multipart_upload_id, tag, finished_count, num_parts);
            }
            catch (Exception & e)
            {
                e.addMessage(fmt::format("while uploading part #{}", task.part_number, dest_key, dest_bucket));
                /// stop other tasks
                has_failed = true;
                throw;
            }

        }

        /// These functions can be called from multiple threads, so derived class needs to take care about synchronization.
        virtual std::unique_ptr<Aws::AmazonWebServiceRequest> makeUploadPartRequest(size_t part_number, size_t part_offset, size_t part_size) const = 0;
        virtual String processUploadPartRequest(Aws::AmazonWebServiceRequest & request) = 0;
    };

    /// Helper class to help implementing copyDataToS3File().
    class CopyDataToFileHelper : public UploadHelper
    {
    public:
        CopyDataToFileHelper(
            const CreateReadBuffer & create_read_buffer_,
            size_t offset_,
            size_t size_,
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            const S3::S3RequestSettings & request_settings_,
            const std::optional<std::map<String, String>> & object_metadata_,
            ThreadPoolCallbackRunnerUnsafe<void> schedule_,
            BlobStorageLogWriterPtr blob_storage_log_)
            : UploadHelper(client_ptr_, dest_bucket_, dest_key_, request_settings_, object_metadata_, schedule_, blob_storage_log_, getLogger("copyDataToS3File"))
            , create_read_buffer(create_read_buffer_)
            , offset(offset_)
            , size(size_)
        {
        }

        void performCopy()
        {
            if (size <= request_settings[S3RequestSetting::max_single_part_upload_size])
                performSinglepartUpload();
            else
                performMultipartUpload();

            if (request_settings[S3RequestSetting::check_objects_after_upload])
                checkObjectAfterUpload();
        }

    private:
        std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer;
        size_t offset;
        size_t size;

        void performSinglepartUpload()
        {
            S3::PutObjectRequest request;
            fillPutRequest(request);
            processPutRequest(request);
        }

        void fillPutRequest(S3::PutObjectRequest & request)
        {
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), offset, size);

            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);
            request.SetContentLength(size);
            request.SetBody(std::make_unique<StdStreamFromReadBuffer>(std::move(read_buffer), size));

            if (object_metadata.has_value())
                request.SetMetadata(object_metadata.value());

            const auto & storage_class_name = request_settings[S3RequestSetting::storage_class_name];
            if (!storage_class_name.value.empty())
                request.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request.SetContentType("binary/octet-stream");

            client_ptr->setKMSHeaders(request);
        }

        void processPutRequest(S3::PutObjectRequest & request)
        {
            size_t max_retries = std::max<UInt64>(request_settings[S3RequestSetting::max_unexpected_write_error_retries].value, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3PutObject);
                if (client_ptr->isClientForDisk())
                    ProfileEvents::increment(ProfileEvents::DiskS3PutObject);

                Stopwatch watch;
                auto outcome = client_ptr->PutObject(request);
                auto elapsed = watch.elapsedMicroseconds();

                if (blob_storage_log)
                    blob_storage_log->addEvent(BlobStorageLogElement::EventType::Upload,
                                               dest_bucket, dest_key, /* local_path_ */ {}, size, elapsed,
                                               outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                               outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

                if (outcome.IsSuccess())
                {
                    Int64 object_size = request.GetContentLength();
                    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Bytes, object_size);
                    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Microseconds, elapsed);
                    LOG_TRACE(
                        log,
                        "Single part upload has completed. Bucket: {}, Key: {}, Object size: {}",
                        dest_bucket,
                        dest_key,
                        object_size);
                    break;
                }

                if (outcome.GetError().GetExceptionName() == "EntityTooLarge" || outcome.GetError().GetExceptionName() == "InvalidRequest")
                {
                    // Can't come here with MinIO, MinIO allows single part upload for large objects.
                    LOG_INFO(
                        log,
                        "Single part upload failed with error {} for Bucket: {}, Key: {}, Object size: {}, will retry with multipart upload",
                        outcome.GetError().GetExceptionName(),
                        dest_bucket,
                        dest_key,
                        size);
                    performMultipartUpload();
                    break;
                }

                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && (retries < max_retries))
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    LOG_INFO(
                        log,
                        "Single part upload failed with NO_SUCH_KEY error for Bucket: {}, Key: {}, Object size: {}, will retry",
                        dest_bucket,
                        dest_key,
                        request.GetContentLength());
                    continue; /// will retry
                }
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Message: {}, Key: {}, Bucket: {}, Object size: {}",
                    outcome.GetError().GetMessage(),
                    dest_key,
                    dest_bucket,
                    request.GetContentLength());
            }
        }

        void performMultipartUpload() { UploadHelper::performMultipartUpload(offset, size); }

        std::unique_ptr<Aws::AmazonWebServiceRequest> makeUploadPartRequest(size_t part_number, size_t part_offset, size_t part_size) const override
        {
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), part_offset, part_size);

            /// Setup request.
            auto request = std::make_unique<S3::UploadPartRequest>();
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetPartNumber(static_cast<int>(part_number));
            request->SetUploadId(multipart_upload_id);
            request->SetContentLength(part_size);
            request->SetBody(std::make_unique<StdStreamFromReadBuffer>(std::move(read_buffer), part_size));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request->SetContentType("binary/octet-stream");

            return request;
        }

        String processUploadPartRequest(Aws::AmazonWebServiceRequest & request) override
        {
            auto & req = typeid_cast<S3::UploadPartRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPart);
            if (client_ptr->isClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPart);

            Stopwatch watch;
            auto outcome = client_ptr->UploadPart(req);
            auto elapsed = watch.elapsedMicroseconds();

            if (blob_storage_log)
                blob_storage_log->addEvent(BlobStorageLogElement::EventType::MultiPartUploadWrite,
                                           dest_bucket, dest_key, /* local_path_ */ {}, size, elapsed,
                                           outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                           outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

            if (!outcome.IsSuccess())
            {
                ProfileEvents::increment(ProfileEvents::WriteBufferFromS3RequestsErrors, 1);
                throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
            }

            return outcome.GetResult().GetETag();
        }
    };

    /// Helper class to help implementing copyS3File().
    class CopyFileHelper : public UploadHelper
    {
    public:
        CopyFileHelper(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & src_bucket_,
            const String & src_key_,
            size_t src_offset_,
            size_t src_size_,
            const String & dest_bucket_,
            const String & dest_key_,
            const S3::S3RequestSettings & request_settings_,
            const ReadSettings & read_settings_,
            const std::optional<std::map<String, String>> & object_metadata_,
            ThreadPoolCallbackRunnerUnsafe<void> schedule_,
            BlobStorageLogWriterPtr blob_storage_log_,
            std::function<void()> fallback_method_)
            : UploadHelper(
                client_ptr_,
                dest_bucket_,
                dest_key_,
                request_settings_,
                object_metadata_,
                schedule_,
                blob_storage_log_,
                getLogger("copyS3File"))
            , src_bucket(src_bucket_)
            , src_key(src_key_)
            , offset(src_offset_)
            , size(src_size_)
            , supports_multipart_copy(client_ptr_->supportsMultiPartCopy())
            , read_settings(read_settings_)
            , fallback_method(std::move(fallback_method_))
        {
        }

        void performCopy()
        {
            LOG_TEST(log, "Copy object {} to {} using native copy", src_key, dest_key);
            bool use_single_operation_copy = !supports_multipart_copy || !request_settings[S3RequestSetting::allow_multipart_copy]
                || (size <= request_settings[S3RequestSetting::max_single_operation_copy_size]);

            if (use_single_operation_copy)
                performSingleOperationCopy();
            else
                performMultipartUploadCopy();

            if (request_settings[S3RequestSetting::check_objects_after_upload])
                checkObjectAfterUpload();
        }

    private:
        const String & src_bucket;
        const String & src_key;
        size_t offset;
        size_t size;
        bool supports_multipart_copy;
        const ReadSettings read_settings;
        std::function<void()> fallback_method;

        void performSingleOperationCopy()
        {
            S3::CopyObjectRequest request;
            fillCopyRequest(request);
            processCopyRequest(request);
        }

        void fillCopyRequest(S3::CopyObjectRequest & request)
        {
            request.SetCopySource(src_bucket + "/" + src_key);
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);

            if (object_metadata.has_value())
            {
                request.SetMetadata(object_metadata.value());
                request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
            }

            const auto & storage_class_name = request_settings[S3RequestSetting::storage_class_name];
            if (!storage_class_name.value.empty())
                request.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request.SetContentType("binary/octet-stream");

            client_ptr->setKMSHeaders(request);
        }

        void processCopyRequest(S3::CopyObjectRequest & request)
        {
            size_t max_retries = std::max<UInt64>(request_settings[S3RequestSetting::max_unexpected_write_error_retries].value, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3CopyObject);
                if (client_ptr->isClientForDisk())
                    ProfileEvents::increment(ProfileEvents::DiskS3CopyObject);

                auto outcome = client_ptr->CopyObject(request);
                if (outcome.IsSuccess())
                {
                    LOG_TRACE(
                        log,
                        "Single operation copy has completed. Bucket: {}, Key: {}, Object size: {}",
                        dest_bucket,
                        dest_key,
                        size);
                    break;
                }

                if (outcome.GetError().GetExceptionName() == "EntityTooLarge" ||
                    outcome.GetError().GetExceptionName() == "InvalidRequest" ||
                    outcome.GetError().GetExceptionName() == "InvalidArgument" ||
                    outcome.GetError().GetExceptionName() == "AccessDenied" ||
                    S3::Client::RetryStrategy::useGCSRewrite(outcome.GetError()))
                {
                    if (!supports_multipart_copy || outcome.GetError().GetExceptionName() == "AccessDenied")
                    {
                        LOG_INFO(
                            log,
                            "Multipart upload using copy is not supported, will try regular upload for Bucket: {}, Key: {}, Object size: "
                            "{}",
                            dest_bucket,
                            dest_key,
                            size);
                        fallback_method();
                        break;
                    }

                    // Can't come here with MinIO, MinIO allows single part upload for large objects.
                    LOG_INFO(
                        log,
                        "Single operation copy failed with error {} for Bucket: {}, Key: {}, Object size: {}, will retry with multipart "
                        "upload copy",
                        outcome.GetError().GetExceptionName(),
                        dest_bucket,
                        dest_key,
                        size);

                    performMultipartUploadCopy();
                    break;
                }

                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && (retries < max_retries))
                {
                    /// TODO: Is it true for copy requests?
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    LOG_INFO(
                        log,
                        "Single operation copy failed with NO_SUCH_KEY error for Bucket: {}, Key: {}, Object size: {}, will retry",
                        dest_bucket,
                        dest_key,
                        size);
                    continue; /// will retry
                }

                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Message: {}, Key: {}, Bucket: {}, Object size: {}",
                    outcome.GetError().GetMessage(),
                    dest_key,
                    dest_bucket,
                    size);
            }
        }

        void performMultipartUploadCopy()
        {
            try
            {
                UploadHelper::performMultipartUpload(offset, size);
            }
            catch (const S3Exception & e)
            {
                if (e.getS3ErrorCode() != Aws::S3::S3Errors::ACCESS_DENIED)
                    throw;

                tryLogCurrentException(log, "Multi part copy failed, trying with regular upload");
                fallback_method();
            }
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> makeUploadPartRequest(size_t part_number, size_t part_offset, size_t part_size) const override
        {
            auto request = std::make_unique<S3::UploadPartCopyRequest>();

            /// Make a copy request to copy a part.
            request->SetCopySource(src_bucket + "/" + src_key);
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetUploadId(multipart_upload_id);
            request->SetPartNumber(static_cast<int>(part_number));
            request->SetCopySourceRange(fmt::format("bytes={}-{}", part_offset, part_offset + part_size - 1));

            return request;
        }

        String processUploadPartRequest(Aws::AmazonWebServiceRequest & request) override
        {
            auto & req = typeid_cast<S3::UploadPartCopyRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPartCopy);
            if (client_ptr->isClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPartCopy);

            auto outcome = client_ptr->UploadPartCopy(req);
            if (!outcome.IsSuccess())
            {
                throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
            }

            return outcome.GetResult().GetCopyPartResult().GetETag();
        }
    };
}


void copyDataToS3File(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    size_t offset,
    size_t size,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const S3::S3RequestSettings & settings,
    BlobStorageLogWriterPtr blob_storage_log,
    ThreadPoolCallbackRunnerUnsafe<void> schedule,
    const std::optional<std::map<String, String>> & object_metadata)
{
    CopyDataToFileHelper helper{
        create_read_buffer,
        offset,
        size,
        dest_s3_client,
        dest_bucket,
        dest_key,
        settings,
        object_metadata,
        schedule,
        blob_storage_log};
    helper.performCopy();
}


void copyS3File(
    std::shared_ptr<const S3::Client> src_s3_client,
    const String & src_bucket,
    const String & src_key,
    size_t src_offset,
    size_t src_size,
    std::shared_ptr<const S3::Client> dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const S3::S3RequestSettings & settings,
    const ReadSettings & read_settings,
    BlobStorageLogWriterPtr blob_storage_log,
    ThreadPoolCallbackRunnerUnsafe<void> schedule,
    const CreateReadBuffer& fallback_file_reader,
    const std::optional<std::map<String, String>> & object_metadata)
{
    if (!dest_s3_client)
        dest_s3_client = src_s3_client;

    std::function<void()> fallback_method = [&] mutable
    {
        copyDataToS3File(
            fallback_file_reader,
            src_offset,
            src_size,
            dest_s3_client,
            dest_bucket,
            dest_key,
            settings,
            blob_storage_log,
            schedule,
            object_metadata);
    };

    if (!settings[S3RequestSetting::allow_native_copy])
    {
        LOG_TRACE(getLogger("copyS3File"), "Native copy is disable for {}", src_key);
        fallback_method();
        return;
    }

    CopyFileHelper helper{
        src_s3_client,
        src_bucket,
        src_key,
        src_offset,
        src_size,
        dest_bucket,
        dest_key,
        settings,
        read_settings,
        object_metadata,
        schedule,
        blob_storage_log,
        std::move(fallback_method)};
    helper.performCopy();
}

}

#endif
