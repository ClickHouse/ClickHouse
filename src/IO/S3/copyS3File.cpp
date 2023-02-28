#include <IO/S3/copyS3File.h>

#if USE_AWS_S3

#include <Common/CoTask.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdStreamFromReadBuffer.h>

#include <IO/S3/Requests.h>

namespace ProfileEvents
{
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


namespace
{
    class S3UploadHelper
    {
    public:
        S3UploadHelper(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            size_t offset_,
            size_t size_,
            const CopyS3FileSettings & copy_settings_,
            const Poco::Logger * log_)
            : client_ptr(client_ptr_)
            , dest_bucket(dest_bucket_)
            , dest_key(dest_key_)
            , offset(offset_)
            , size(size_)
            , request_settings(copy_settings_.request_settings)
            , upload_settings(request_settings.getUploadSettings())
            , object_metadata(copy_settings_.object_metadata)
            , for_disk_s3(copy_settings_.for_disk_s3)
            , log(log_)
        {
        }

        virtual ~S3UploadHelper() = default;

        /// Main function.
        Co::Task<> upload() const
        {
            if (size == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Copy size is not set");

            if (shouldUseMultipartUpload())
            {
                co_await performMultipartUpload();
            }
            else
            {
                if (performSingleOperationUpload() == SingleOperationResult::TRY_SWITCH_TO_MULTIPART)
                    co_await performMultipartUpload();
            }

            if (request_settings.check_objects_after_upload)
                checkObjectAfterUpload();
        }

    protected:
        const std::shared_ptr<const S3::Client> client_ptr;
        const String & dest_bucket;
        const String & dest_key;
        size_t offset;
        size_t size;
        const S3Settings::RequestSettings & request_settings;
        const S3Settings::RequestSettings::PartUploadSettings & upload_settings;
        const std::optional<std::map<String, String>> & object_metadata;
        const bool for_disk_s3;
        const Poco::Logger * log;

        /// Decides if a multipart upload should be used.
        virtual bool shouldUseMultipartUpload() const = 0;

        enum class SingleOperationResult
        {
            OK,                      /// Singlepart upload has succeeded.
            TRY_SWITCH_TO_MULTIPART, /// Singlepart upload has failed but it seems we can try using multipart upload instead.
            /// In other cases performSingleOperationUpload() throws exceptions.
        };

        /// Copies the source in a single copy operation.
        SingleOperationResult performSingleOperationUpload() const
        {
            auto req = makeSingleOperationUploadRequest();

            size_t max_retries = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                LOG_TRACE(log, "Uploading \"{}\" using singlepart operation. Bucket: {}, Size: {}", dest_key, dest_bucket, size);

                auto [ok, error] = processSingleOperationUploadRequest(*req);

                if (ok)
                {
                    LOG_TRACE(log, "Completed singlepart upload to \"{}\". Bucket: {}, Size: {}", dest_key, dest_bucket, size);
                    return SingleOperationResult::OK;
                }

                if ((error.GetExceptionName() == "EntityTooLarge" || error.GetExceptionName() == "InvalidRequest") && size)
                {
                    // Can't come here with MinIO, MinIO allows single part upload for large objects.
                    LOG_INFO(log, "Failed singlepart upload to \"{}\": {}. Bucket: {}, Size: {}. Will retry with multipart upload",
                             dest_key, error.GetExceptionName(), dest_bucket, size);
                    return SingleOperationResult::TRY_SWITCH_TO_MULTIPART;
                }

                bool can_retry = (retries < max_retries);
                if ((error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && can_retry)
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    LOG_INFO(log, "Failed singlepart upload to \"{}\": {}. Bucket: {}, Size: {}. Will retry",
                             dest_key, error.GetMessage(), dest_bucket, size);
                    continue; /// will retry
                }

                throw S3Exception(error.GetErrorType(),
                                  "Failed singlepart upload to \"{}\": {}. Bucket: {}, Size: {}",
                                  dest_key, error.GetMessage(), dest_bucket, size);
            }
        }

        virtual std::unique_ptr<Aws::AmazonWebServiceRequest> makeSingleOperationUploadRequest() const = 0;
        virtual std::pair<bool, Aws::S3::S3Error> processSingleOperationUploadRequest(const Aws::AmazonWebServiceRequest & request) const = 0;

        String createMultipartUpload(size_t part_size, size_t num_parts) const
        {
            S3::CreateMultipartUploadRequest request;
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request.SetContentType("binary/octet-stream");

            if (object_metadata.has_value())
                request.SetMetadata(object_metadata.value());

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);

            auto outcome = client_ptr->CreateMultipartUpload(request);

            if (outcome.IsSuccess())
            {
                String multipart_upload_id = outcome.GetResult().GetUploadId();
                LOG_TRACE(log, "Started multipart upload of {} parts to \"{}\". Part size: {}, Total size: {}, Bucket: {}, Upload id: {}",
                        num_parts, dest_key, part_size, size, dest_bucket, multipart_upload_id);
                return multipart_upload_id;
            }

            throw S3Exception(
                outcome.GetError().GetErrorType(),
                "Couldn't start multipart upload of {} parts to \"{}\": {}. Part size: {}, Total size: {}, Bucket: {}",
                num_parts, dest_key, outcome.GetError().GetMessage(), part_size, size, dest_bucket);

        }

        void completeMultipartUpload(const String & multipart_upload_id, const Strings & part_tags) const
        {
            size_t num_parts = part_tags.size();
            LOG_TRACE(log, "Completing multipart upload to \"{}\". Bucket: {}, Parts: {}, Upload id: {}",
                      dest_key, dest_bucket, num_parts, multipart_upload_id);

            if (!num_parts)
                throw Exception(ErrorCodes::S3_ERROR, "Failed to complete multipart upload. No parts have uploaded");

            S3::CompleteMultipartUploadRequest request;
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);
            request.SetUploadId(multipart_upload_id);

            Aws::S3::Model::CompletedMultipartUpload multipart_upload;
            for (size_t i = 0; i < num_parts; ++i)
            {
                Aws::S3::Model::CompletedPart part;
                multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(static_cast<int>(i + 1)));
            }

            request.SetMultipartUpload(multipart_upload);

            size_t max_retries = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
                if (for_disk_s3)
                    ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);

                auto outcome = client_ptr->CompleteMultipartUpload(request);

                if (outcome.IsSuccess())
                {
                    LOG_TRACE(log, "Completed multipart upload to \"{}\". Upload id: {}", dest_key, multipart_upload_id);
                    break;
                }

                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && (retries < max_retries))
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    /// BTW, NO_SUCH_UPLOAD is expected error and we shouldn't retry it
                    LOG_INFO(log, "Couldn't complete multipart upload to \"{}\": {}. Bucket: {}, Parts: {}, Upload id: {}. Will retry",
                             dest_key, outcome.GetError().GetMessage(), dest_bucket, num_parts, multipart_upload_id);
                    continue; /// will retry
                }

                throw S3Exception(outcome.GetError().GetErrorType(),
                                  "Couldn't complete multipart upload to \"{}\": {}. Bucket: {}, Parts: {}, Upload id: {}",
                                  dest_key, outcome.GetError().GetMessage(), dest_bucket, num_parts, multipart_upload_id);
            }
        }

        void abortMultipartUpload(const String & multipart_upload_id, size_t num_parts, size_t num_parts_uploaded) const
        {
            LOG_TRACE(log, "Aborting multipart upload to \"{}\". Bucket: {}, Parts uploaded: {} / {}, Upload id: {}",
                      dest_key, dest_bucket, num_parts_uploaded, num_parts, multipart_upload_id);
            S3::AbortMultipartUploadRequest abort_request;
            abort_request.SetBucket(dest_bucket);
            abort_request.SetKey(dest_key);
            abort_request.SetUploadId(multipart_upload_id);
            client_ptr->AbortMultipartUpload(abort_request);
        }

        void checkObjectAfterUpload() const
        {
            LOG_TRACE(log, "Checking object {} exists after upload", dest_key);
            S3::checkObjectExistsAndHasSize(*client_ptr, dest_bucket, dest_key, {}, size, request_settings, {}, "Immediately after upload");
            LOG_TRACE(log, "Object {} exists after upload", dest_key);
        }

        Co::Task<> performMultipartUpload() const
        {
            size_t part_size = calculatePartSize();
            size_t num_parts = calculateNumParts(part_size);

            String multipart_upload_id = createMultipartUpload(part_size, num_parts);

            std::atomic<size_t> num_parts_uploaded = 0; /// Used only to show progress in the logging messages.
            std::atomic<bool> multipart_upload_aborted = false; /// Used to avoid repeating AbortMultipartUpload requests if multiple parts fail.

            bool multipart_upload_completed = false;
            SCOPE_EXIT_SAFE({
                if (!multipart_upload_completed) /// If this coroutine exits without exceptions then `multipart_upload_completed` must be true.
                    abortMultipartUpload(multipart_upload_id, num_parts, num_parts_uploaded.load());
            });

            std::vector<Co::Task<String>> part_uploads;

            for (size_t part_number = 1; part_number <= num_parts; ++part_number)
            {
                size_t position = offset + (part_number - 1) * part_size;
                /// `current_part_size` is either `part_size` or smaller if it's the final part.
                size_t current_part_size = std::min(part_size, offset + size - position);

                /// It's safe to pass `num_parts_uploaded` and `multipart_upload_aborted` to the `part_uploads` tasks by reference because
                /// those tasks will be finished or cancelled before those variables go out of scope, `co_await Co::parallel()` guarantees that.
                part_uploads.emplace_back(uploadPart(multipart_upload_id, part_number, position, current_part_size, num_parts, num_parts_uploaded, multipart_upload_aborted));
            }

            auto part_tags = co_await Co::parallel(std::move(part_uploads));

            if (multipart_upload_aborted.load()) /// `co_await Co::Parallel()` must have thrown an exception in this case
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Aborted multipart upload came to completion. This must not happen");

            completeMultipartUpload(multipart_upload_id, part_tags);
            multipart_upload_completed = true;
        }

        size_t calculatePartSize() const
        {
            if (!size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chosen multipart upload for an empty file. This must not happen");

            auto max_part_number = upload_settings.max_part_number;
            auto min_upload_part_size = upload_settings.min_upload_part_size;
            auto max_upload_part_size = upload_settings.max_upload_part_size;

            if (!max_part_number)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_part_number must not be 0");
            else if (!min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "min_upload_part_size must not be 0");
            else if (max_upload_part_size < min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_upload_part_size must not be less than min_upload_part_size");

            size_t part_size = min_upload_part_size;
            size_t num_parts = (size + part_size - 1) / part_size;

            if (num_parts > max_part_number)
            {
                part_size = (size + max_part_number - 1) / max_part_number;
                num_parts = (size + part_size - 1) / part_size;
            }

            if (part_size > max_upload_part_size)
            {
                part_size = max_upload_part_size;
                num_parts = (size + part_size - 1) / part_size;
            }

            if (num_parts < 1 || num_parts > max_part_number || part_size < min_upload_part_size || part_size > max_upload_part_size)
            {
                String msg;
                if (num_parts < 1)
                    msg = "Number of parts is zero";
                else if (num_parts > max_part_number)
                    msg = fmt::format("Number of parts exceeds {}", num_parts, max_part_number);
                else if (part_size < min_upload_part_size)
                    msg = fmt::format("Size of a part is less than {}", part_size, min_upload_part_size);
                else
                    msg = fmt::format("Size of a part exceeds {}", part_size, max_upload_part_size);

                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "{} while writing {} bytes to S3. Check max_part_number = {}, min_upload_part_size = {}, max_upload_part_size = {}",
                    msg, size, max_part_number, min_upload_part_size, max_upload_part_size);
            }

            /// We've calculated the size of a normal part (the final part can be smaller).
            return part_size;
        }

        size_t calculateNumParts(size_t part_size) const
        {
            return (size + part_size - 1) / part_size;
        }

        Co::Task<String> uploadPart(
            const String & multipart_upload_id,
            size_t part_number,
            size_t part_offset,
            size_t part_size,
            size_t num_parts,
            std::atomic<size_t> & num_parts_uploaded,
            std::atomic<bool> & multipart_upload_aborted) const
        {
            LOG_TRACE(log, "Writing part #{} to \"{}\". Part offset: {}, Part size: {}, Upload id: {}",
                      part_number, dest_key, part_offset, part_size, multipart_upload_id);

            if (!part_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part size if empty. This must not happen");

            bool upload_part_completed = false;
            SCOPE_EXIT_SAFE({
                if (!upload_part_completed && !multipart_upload_aborted.exchange(true)) /// If this coroutine exits without exceptions then `upload_part_completed` must be true.
                    abortMultipartUpload(multipart_upload_id, num_parts, num_parts_uploaded.load());
            });

            auto req = makeUploadPartRequest(multipart_upload_id, part_number, part_offset, part_size);
            auto [part_tag, error] = processUploadPartRequest(*req);

            if (!part_tag)
            {
                throw S3Exception(error.GetErrorType(),
                                  "Couldn't upload part #{} to \"{}\". Part offset: {}, Part size: {}, Upload id: {}",
                                  part_number, dest_key, part_offset, part_size, multipart_upload_id);
            }

            upload_part_completed = true;
            ++num_parts_uploaded;
            LOG_TRACE(log, "Finished writing part #{} to \"{}\". Parts ready: {} / {}, Upload id: {}",
                        part_number, dest_key, num_parts_uploaded.load(), num_parts, multipart_upload_id);
            co_return *part_tag;
        }

        virtual std::unique_ptr<Aws::AmazonWebServiceRequest>
        makeUploadPartRequest(const String & multipart_upload_id, size_t part_number, size_t part_offset, size_t part_size) const = 0;

        virtual std::pair<std::optional<String> /* part_tag */, Aws::S3::S3Error>
        processUploadPartRequest(const Aws::AmazonWebServiceRequest & request) const = 0;
    };

    /// Helper class to help implementing copyDataToS3File().
    class CopyDataToS3FileHelper : public S3UploadHelper
    {
    public:
        CopyDataToS3FileHelper(
            const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer_,
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            const CopyS3FileSettings & copy_settings_)
            : CopyDataToS3FileHelper(
                create_read_buffer_, client_ptr_, dest_bucket_, dest_key_, copy_settings_.offset, copy_settings_.size, copy_settings_)
        {
        }

        CopyDataToS3FileHelper(
            const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer_,
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            size_t offset_,
            size_t size_,
            const CopyS3FileSettings & copy_settings_)
            : S3UploadHelper(client_ptr_, dest_bucket_, dest_key_, offset_, size_, copy_settings_, &Poco::Logger::get("copyDataToS3File"))
            , create_read_buffer(create_read_buffer_)
        {
        }

    private:
        std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer;

        bool shouldUseMultipartUpload() const override
        {
            return size > request_settings.getUploadSettings().max_single_part_upload_size;
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> makeSingleOperationUploadRequest() const override
        {
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), offset, size);

            auto request = std::make_unique<S3::PutObjectRequest>();

            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetContentLength(size);
            request->SetBody(std::make_unique<StdStreamFromReadBuffer>(std::move(read_buffer), size));

            if (object_metadata.has_value())
                request->SetMetadata(object_metadata.value());

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request->SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request->SetContentType("binary/octet-stream");
            return request;
        }

        std::pair<bool, Aws::S3::S3Error> processSingleOperationUploadRequest(const Aws::AmazonWebServiceRequest & request) const override
        {
            ProfileEvents::increment(ProfileEvents::S3PutObject);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3PutObject);

            const auto & req = typeid_cast<const S3::PutObjectRequest &>(request);
            auto outcome = client_ptr->PutObject(req);
            if (outcome.IsSuccess())
                return {true, {}};
            return {false, outcome.GetError()};
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest>
        makeUploadPartRequest(const String & multipart_upload_id, size_t part_number, size_t part_offset, size_t part_size) const override
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

        std::pair<std::optional<String>, Aws::S3::S3Error> processUploadPartRequest(const Aws::AmazonWebServiceRequest & request) const override
        {
            const auto & req = typeid_cast<const S3::UploadPartRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPart);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPart);

            auto outcome = client_ptr->UploadPart(req);
            if (outcome.IsSuccess())
                return {outcome.GetResult().GetETag(), {}};

            return {{}, outcome.GetError()};
        }
    };

    /// Helper class to help implementing copyS3File().
    class CopyS3FileHelper : public S3UploadHelper
    {
    public:
        CopyS3FileHelper(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & src_bucket_,
            const String & src_key_,
            const String & dest_bucket_,
            const String & dest_key_,
            const CopyS3FileSettings & copy_settings_)
            : S3UploadHelper(client_ptr_, dest_bucket_, dest_key_, copy_settings_.offset, copy_settings_.size, copy_settings_, &Poco::Logger::get("copyS3File"))
            , src_bucket(src_bucket_)
            , src_key(src_key_)
            , whole_file(copy_settings_.whole_file)
        {
        }

        /// Main function.
        Co::Task<> upload()
        {
            if (whole_file)
            {
                if (offset != 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "A non-zero starting offset is specified while the whole file must be copied");

                if (size == static_cast<size_t>(-1))
                {
                    /// The size is not specified, but we can get it from the size of the source object.
                    size = S3::getObjectSize(*client_ptr, src_bucket, src_key, {}, request_settings, for_disk_s3);
                }
            }

            co_await S3UploadHelper::upload();
        }


    private:
        const String & src_bucket;
        const String & src_key;
        const bool whole_file;

        bool shouldUseMultipartUpload() const override
        {
            return (size > request_settings.getUploadSettings().max_single_operation_copy_size) || !whole_file;
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> makeSingleOperationUploadRequest() const override
        {
            auto request = std::make_unique<S3::CopyObjectRequest>();

            request->SetCopySource(src_bucket + "/" + src_key);
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);

            if (object_metadata.has_value())
            {
                request->SetMetadata(object_metadata.value());
                request->SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
            }

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request->SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request->SetContentType("binary/octet-stream");
            return request;
        }

        std::pair<bool, Aws::S3::S3Error> processSingleOperationUploadRequest(const Aws::AmazonWebServiceRequest & request) const override
        {
            const auto & req = typeid_cast<const S3::CopyObjectRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3CopyObject);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3CopyObject);

            auto outcome = client_ptr->CopyObject(req);
            if (outcome.IsSuccess())
                return {true, {}};
            return {false, outcome.GetError()};
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest>
        makeUploadPartRequest(const String & multipart_upload_id, size_t part_number, size_t part_offset, size_t part_size) const override
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

        std::pair<std::optional<String>, Aws::S3::S3Error> processUploadPartRequest(const Aws::AmazonWebServiceRequest & request) const override
        {
            const auto & req = typeid_cast<const S3::UploadPartCopyRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPartCopy);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPartCopy);

            auto outcome = client_ptr->UploadPartCopy(req);
            if (outcome.IsSuccess())
                return {outcome.GetResult().GetCopyPartResult().GetETag(), {}};
            return {{}, outcome.GetError()};
        }
    };
}


Co::Task<> copyDataToS3FileAsync(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer,
    const std::shared_ptr<const S3::Client> dest_s3_client,
    const String dest_bucket,
    const String dest_key,
    const CopyS3FileSettings copy_settings)
{
    CopyDataToS3FileHelper helper{create_read_buffer, dest_s3_client, dest_bucket, dest_key, copy_settings};
    co_await helper.upload();
}

Co::Task<> copyS3FileAsync(
    const std::shared_ptr<const S3::Client> s3_client,
    const String src_bucket,
    const String src_key,
    const String dest_bucket,
    const String dest_key,
    const CopyS3FileSettings copy_settings)
{
    if (copy_settings.size == 0)
    {
        /// A special case. We have to copy zero bytes, however it's not allowed to use multipart upload for zero size.
        /// So we'll just send one PutObject request here in CopyDataToS3FileHelper.
        auto create_empty_read_buffer = []() -> std::unique_ptr<SeekableReadBuffer> { return std::make_unique<ReadBufferFromMemory>(static_cast<char *>(nullptr), 0); };
        CopyDataToS3FileHelper helper(create_empty_read_buffer, s3_client, dest_bucket, dest_key, 0, 0, copy_settings);
        co_await helper.upload();
    }
    else
    {
        /// Generic case.
        CopyS3FileHelper helper{s3_client, src_bucket, src_key, dest_bucket, dest_key, copy_settings};
        co_await helper.upload();
    }
}

void copyS3File(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & src_bucket,
    const String & src_key,
    const String & dest_bucket,
    const String & dest_key,
    const CopyS3FileSettings & copy_settings,
    const ThreadPoolCallbackRunner<void> & scheduler)
{
    copyS3FileAsync(s3_client, src_bucket, src_key, dest_bucket, dest_key, copy_settings).syncRun(Co::Scheduler{scheduler});
}

void copyDataToS3File(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const CopyS3FileSettings & copy_settings,
    const ThreadPoolCallbackRunner<void> & scheduler)
{
    copyDataToS3FileAsync(create_read_buffer, dest_s3_client, dest_bucket, dest_key, copy_settings).syncRun(Co::Scheduler(scheduler));
}

}

#endif
