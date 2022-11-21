#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Common/ProfileEvents.h>

#if USE_AWS_S3

#include <IO/S3Common.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>

#include <Common/getRandomASCIIString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/MultiVersion.h>


namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3HeadObject;
    extern const Event S3ListObjects;
    extern const Event S3CopyObject;
    extern const Event S3CreateMultipartUpload;
    extern const Event S3UploadPartCopy;
    extern const Event S3AbortMultipartUpload;
    extern const Event S3CompleteMultipartUpload;

    extern const Event DiskS3DeleteObjects;
    extern const Event DiskS3HeadObject;
    extern const Event DiskS3ListObjects;
    extern const Event DiskS3CopyObject;
    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3UploadPartCopy;
    extern const Event DiskS3AbortMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
        || error == Aws::S3::S3Errors::NO_SUCH_KEY;
}

template <typename Result, typename Error>
void throwIfError(const Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw S3Exception(fmt::format("{} (Code: {})", err.GetMessage(), static_cast<size_t>(err.GetErrorType())), err.GetErrorType());
    }
}

template <typename Result, typename Error>
void throwIfUnexpectedError(const Aws::Utils::Outcome<Result, Error> & response, bool if_exists)
{
    /// In this case even if absence of key may be ok for us,
    /// the log will be polluted with error messages from aws sdk.
    /// Looks like there is no way to suppress them.

    if (!response.IsSuccess() && (!if_exists || !isNotFoundError(response.GetError().GetErrorType())))
    {
        const auto & err = response.GetError();
        throw S3Exception(err.GetErrorType(), "{} (Code: {})", err.GetMessage(), static_cast<size_t>(err.GetErrorType()));
    }
}

template <typename Result, typename Error>
void logIfError(const Aws::Utils::Outcome<Result, Error> & response, std::function<String()> && msg)
{
    try
    {
        throwIfError(response);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, msg());
    }
}

}

std::string S3ObjectStorage::generateBlobNameForPath(const std::string & /* path */)
{
    /// Path to store the new S3 object.

    /// Total length is 32 a-z characters for enough randomness.
    /// First 3 characters are used as a prefix for
    /// https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/

    constexpr size_t key_name_total_size = 32;
    constexpr size_t key_name_prefix_size = 3;

    /// Path to store new S3 object.
    return fmt::format("{}/{}",
        getRandomASCIIString(key_name_prefix_size),
        getRandomASCIIString(key_name_total_size - key_name_prefix_size));
}

Aws::S3::Model::HeadObjectOutcome S3ObjectStorage::requestObjectHeadData(const std::string & bucket_from, const std::string & key) const
{
    auto client_ptr = client.get();

    ProfileEvents::increment(ProfileEvents::S3HeadObject);
    ProfileEvents::increment(ProfileEvents::DiskS3HeadObject);
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket_from);
    request.SetKey(key);

    return client_ptr->HeadObject(request);
}

bool S3ObjectStorage::exists(const StoredObject & object) const
{
    auto object_head = requestObjectHeadData(bucket, object.absolute_path);
    if (!object_head.IsSuccess())
    {
        if (object_head.GetError().GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND)
            return false;

        throwIfError(object_head);
    }
    return true;
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    assert(!objects[0].getPathKeyForCache().empty());

    ReadSettings disk_read_settings = patchSettings(read_settings);

    auto settings_ptr = s3_settings.get();

    auto read_buffer_creator =
        [this, settings_ptr, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::shared_ptr<ReadBufferFromFileBase>
    {
        return std::make_shared<ReadBufferFromS3>(
            client.get(),
            bucket,
            path,
            version_id,
            settings_ptr->request_settings,
            disk_read_settings,
            /* use_external_buffer */true,
            /* offset */0,
            read_until_position,
            /* restricted_seek */true);
    };

    auto s3_impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        objects,
        disk_read_settings);

    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto & reader = getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, disk_read_settings, std::move(s3_impl));
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(s3_impl), disk_read_settings);
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings_ptr->min_bytes_for_seek);
    }
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = s3_settings.get();
    return std::make_unique<ReadBufferFromS3>(
        client.get(),
        bucket,
        object.absolute_path,
        version_id,
        settings_ptr->request_settings,
        patchSettings(read_settings));
}

std::unique_ptr<WriteBufferFromFileBase> S3ObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode, // S3 doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    WriteSettings disk_write_settings = IObjectStorage::patchSettings(write_settings);

    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3 doesn't support append to files");

    auto settings_ptr = s3_settings.get();
    ThreadPoolCallbackRunner<void> scheduler;
    if (write_settings.s3_allow_parallel_part_upload)
        scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "VFSWrite");

    auto s3_buffer = std::make_unique<WriteBufferFromS3>(
        client.get(),
        bucket,
        object.absolute_path,
        settings_ptr->request_settings,
        attributes,
        buf_size,
        std::move(scheduler),
        disk_write_settings);

    return std::make_unique<WriteIndirectBufferFromRemoteFS>(
        std::move(s3_buffer), std::move(finalize_callback), object.absolute_path);
}

void S3ObjectStorage::findAllFiles(const std::string & path, RelativePathsWithSize & children, int max_keys) const
{
    auto settings_ptr = s3_settings.get();
    auto client_ptr = client.get();

    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket);
    request.SetPrefix(path);
    if (max_keys)
        request.SetMaxKeys(max_keys);
    else
        request.SetMaxKeys(settings_ptr->list_object_keys_size);

    Aws::S3::Model::ListObjectsV2Outcome outcome;
    do
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);
        outcome = client_ptr->ListObjectsV2(request);
        throwIfError(outcome);

        auto result = outcome.GetResult();
        auto objects = result.GetContents();

        if (objects.empty())
            break;

        for (const auto & object : objects)
            children.emplace_back(object.GetKey(), object.GetSize());

        if (max_keys)
        {
            int keys_left = max_keys - static_cast<int>(children.size());
            if (keys_left <= 0)
                break;
            request.SetMaxKeys(keys_left);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void S3ObjectStorage::getDirectoryContents(const std::string & path,
    RelativePathsWithSize & files,
    std::vector<std::string> & directories) const
{
    auto settings_ptr = s3_settings.get();
    auto client_ptr = client.get();

    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket);
    /// NOTE: if you do "ls /foo" instead of "ls /foo/" over S3 with this API
    /// it will return only "/foo" itself without any underlying nodes.
    if (path.ends_with("/"))
        request.SetPrefix(path);
    else
        request.SetPrefix(path + "/");
    request.SetMaxKeys(settings_ptr->list_object_keys_size);
    request.SetDelimiter("/");

    Aws::S3::Model::ListObjectsV2Outcome outcome;
    do
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);
        outcome = client_ptr->ListObjectsV2(request);
        throwIfError(outcome);

        auto result = outcome.GetResult();
        auto result_objects = result.GetContents();
        auto result_common_prefixes = result.GetCommonPrefixes();

        if (result_objects.empty() && result_common_prefixes.empty())
            break;

        for (const auto & object : result_objects)
            files.emplace_back(object.GetKey(), object.GetSize());

        for (const auto & common_prefix : result_common_prefixes)
        {
            std::string directory = common_prefix.GetPrefix();
            /// Make it compatible with std::filesystem::path::filename()
            trimRight(directory, '/');
            directories.emplace_back(directory);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void S3ObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    auto client_ptr = client.get();

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    ProfileEvents::increment(ProfileEvents::DiskS3DeleteObjects);
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(object.absolute_path);
    auto outcome = client_ptr->DeleteObject(request);

    throwIfUnexpectedError(outcome, if_exists);

    LOG_TRACE(log, "Object with path {} was removed from S3", object.absolute_path);
}

void S3ObjectStorage::removeObjectsImpl(const StoredObjects & objects, bool if_exists)
{
    if (objects.empty())
        return;

    if (!s3_capabilities.support_batch_delete)
    {
        for (const auto & object : objects)
            removeObjectImpl(object, if_exists);
    }
    else
    {
        auto client_ptr = client.get();
        auto settings_ptr = s3_settings.get();

        size_t chunk_size_limit = settings_ptr->objects_chunk_size_to_delete;
        size_t current_position = 0;

        while (current_position < objects.size())
        {
            std::vector<Aws::S3::Model::ObjectIdentifier> current_chunk;
            String keys;
            for (; current_position < objects.size() && current_chunk.size() < chunk_size_limit; ++current_position)
            {
                Aws::S3::Model::ObjectIdentifier obj;
                obj.SetKey(objects[current_position].absolute_path);
                current_chunk.push_back(obj);

                if (!keys.empty())
                    keys += ", ";
                keys += objects[current_position].absolute_path;
            }

            Aws::S3::Model::Delete delkeys;
            delkeys.SetObjects(current_chunk);

            ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
            ProfileEvents::increment(ProfileEvents::DiskS3DeleteObjects);
            Aws::S3::Model::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);
            auto outcome = client_ptr->DeleteObjects(request);

            throwIfUnexpectedError(outcome, if_exists);

            LOG_TRACE(log, "Objects with paths [{}] were removed from S3", keys);
        }
    }
}

void S3ObjectStorage::removeObject(const StoredObject & object)
{
    removeObjectImpl(object, false);
}

void S3ObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectImpl(object, true);
}

void S3ObjectStorage::removeObjects(const StoredObjects & objects)
{
    removeObjectsImpl(objects, false);
}

void S3ObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    removeObjectsImpl(objects, true);
}

ObjectMetadata S3ObjectStorage::getObjectMetadata(const std::string & path) const
{
    ObjectMetadata result;

    auto object_head = requestObjectHeadData(bucket, path);
    throwIfError(object_head);

    auto & object_head_result = object_head.GetResult();
    result.size_bytes = object_head_result.GetContentLength();
    result.last_modified = object_head_result.GetLastModified().Millis();
    result.attributes = object_head_result.GetMetadata();

    return result;
}

void S3ObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    /// Shortcut for S3
    if (auto * dest_s3 = dynamic_cast<S3ObjectStorage * >(&object_storage_to); dest_s3 != nullptr)
    {
        copyObjectImpl(bucket, object_from.absolute_path, dest_s3->bucket, object_to.absolute_path, {}, object_to_attributes);
    }
    else
    {
        IObjectStorage::copyObjectToAnotherObjectStorage(object_from, object_to, object_storage_to, object_to_attributes);
    }
}

void S3ObjectStorage::copyObjectImpl(
    const String & src_bucket,
    const String & src_key,
    const String & dst_bucket,
    const String & dst_key,
    std::optional<Aws::S3::Model::HeadObjectResult> head,
    std::optional<ObjectAttributes> metadata) const
{
    auto client_ptr = client.get();

    ProfileEvents::increment(ProfileEvents::S3CopyObject);
    ProfileEvents::increment(ProfileEvents::DiskS3CopyObject);
    Aws::S3::Model::CopyObjectRequest request;
    request.SetCopySource(src_bucket + "/" + src_key);
    request.SetBucket(dst_bucket);
    request.SetKey(dst_key);
    if (metadata)
    {
        request.SetMetadata(*metadata);
        request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
    }

    auto outcome = client_ptr->CopyObject(request);

    if (!outcome.IsSuccess() && outcome.GetError().GetExceptionName() == "EntityTooLarge")
    { // Can't come here with MinIO, MinIO allows single part upload for large objects.
        copyObjectMultipartImpl(src_bucket, src_key, dst_bucket, dst_key, head, metadata);
        return;
    }

    throwIfError(outcome);

    auto settings_ptr = s3_settings.get();
    if (settings_ptr->request_settings.check_objects_after_upload)
    {
        auto object_head = requestObjectHeadData(dst_bucket, dst_key);
        if (!object_head.IsSuccess())
            throw Exception(ErrorCodes::S3_ERROR, "Object {} from bucket {} disappeared immediately after upload, it's a bug in S3 or S3 API.", dst_key, dst_bucket);
    }

}

void S3ObjectStorage::copyObjectMultipartImpl(
    const String & src_bucket,
    const String & src_key,
    const String & dst_bucket,
    const String & dst_key,
    std::optional<Aws::S3::Model::HeadObjectResult> head,
    std::optional<ObjectAttributes> metadata) const
{
    if (!head)
        head = requestObjectHeadData(src_bucket, src_key).GetResult();

    auto settings_ptr = s3_settings.get();
    auto client_ptr = client.get();
    size_t size = head->GetContentLength();

    String multipart_upload_id;

    {
        ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
        ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);
        Aws::S3::Model::CreateMultipartUploadRequest request;
        request.SetBucket(dst_bucket);
        request.SetKey(dst_key);
        if (metadata)
            request.SetMetadata(*metadata);

        auto outcome = client_ptr->CreateMultipartUpload(request);

        throwIfError(outcome);

        multipart_upload_id = outcome.GetResult().GetUploadId();
    }

    std::vector<String> part_tags;

    size_t upload_part_size = settings_ptr->request_settings.min_upload_part_size;
    for (size_t position = 0, part_number = 1; position < size; ++part_number, position += upload_part_size)
    {
        ProfileEvents::increment(ProfileEvents::S3UploadPartCopy);
        ProfileEvents::increment(ProfileEvents::DiskS3UploadPartCopy);
        Aws::S3::Model::UploadPartCopyRequest part_request;
        part_request.SetCopySource(src_bucket + "/" + src_key);
        part_request.SetBucket(dst_bucket);
        part_request.SetKey(dst_key);
        part_request.SetUploadId(multipart_upload_id);
        part_request.SetPartNumber(static_cast<int>(part_number));
        part_request.SetCopySourceRange(fmt::format("bytes={}-{}", position, std::min(size, position + upload_part_size) - 1));

        auto outcome = client_ptr->UploadPartCopy(part_request);
        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::S3AbortMultipartUpload);
            ProfileEvents::increment(ProfileEvents::DiskS3AbortMultipartUpload);
            Aws::S3::Model::AbortMultipartUploadRequest abort_request;
            abort_request.SetBucket(dst_bucket);
            abort_request.SetKey(dst_key);
            abort_request.SetUploadId(multipart_upload_id);
            client_ptr->AbortMultipartUpload(abort_request);
            // In error case we throw exception later with first error from UploadPartCopy
        }
        throwIfError(outcome);

        auto etag = outcome.GetResult().GetCopyPartResult().GetETag();
        part_tags.push_back(etag);
    }

    {
        ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
        ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);
        Aws::S3::Model::CompleteMultipartUploadRequest req;
        req.SetBucket(dst_bucket);
        req.SetKey(dst_key);
        req.SetUploadId(multipart_upload_id);

        Aws::S3::Model::CompletedMultipartUpload multipart_upload;
        for (size_t i = 0; i < part_tags.size(); ++i)
        {
            Aws::S3::Model::CompletedPart part;
            multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(static_cast<int>(i) + 1));
        }

        req.SetMultipartUpload(multipart_upload);

        auto outcome = client_ptr->CompleteMultipartUpload(req);

        throwIfError(outcome);
    }

    if (settings_ptr->request_settings.check_objects_after_upload)
    {
        auto object_head = requestObjectHeadData(dst_bucket, dst_key);
        if (!object_head.IsSuccess())
            throw Exception(ErrorCodes::S3_ERROR, "Object {} from bucket {} disappeared immediately after upload, it's a bug in S3 or S3 API.", dst_key, dst_bucket);
    }

}

void S3ObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from, const StoredObject & object_to, std::optional<ObjectAttributes> object_to_attributes)
{
    auto head = requestObjectHeadData(bucket, object_from.absolute_path).GetResult();
    static constexpr int64_t multipart_upload_threashold = 5UL * 1024 * 1024 * 1024;

    if (head.GetContentLength() >= multipart_upload_threashold)
    {
        copyObjectMultipartImpl(
            bucket, object_from.absolute_path, bucket, object_to.absolute_path, head, object_to_attributes);
    }
    else
    {
        copyObjectImpl(
            bucket, object_from.absolute_path, bucket, object_to.absolute_path, head, object_to_attributes);
    }
}

void S3ObjectStorage::setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_)
{
    s3_settings.set(std::move(s3_settings_));
}

void S3ObjectStorage::setNewClient(std::unique_ptr<Aws::S3::S3Client> && client_)
{
    client.set(std::move(client_));
}

void S3ObjectStorage::shutdown()
{
    auto client_ptr = client.get();
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    const_cast<Aws::S3::S3Client &>(*client_ptr).DisableRequestProcessing();
}

void S3ObjectStorage::startup()
{
    auto client_ptr = client.get();

    /// Need to be enabled if it was disabled during shutdown() call.
    const_cast<Aws::S3::S3Client &>(*client_ptr).EnableRequestProcessing();
}

void S3ObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto new_s3_settings = getSettings(config, config_prefix, context);
    auto new_client = getClient(config, config_prefix, context, *new_s3_settings);
    s3_settings.set(std::move(new_s3_settings));
    client.set(std::move(new_client));
    applyRemoteThrottlingSettings(context);
}

std::unique_ptr<IObjectStorage> S3ObjectStorage::cloneObjectStorage(
    const std::string & new_namespace, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto new_s3_settings = getSettings(config, config_prefix, context);
    auto new_client = getClient(config, config_prefix, context, *new_s3_settings);
    return std::make_unique<S3ObjectStorage>(
        std::move(new_client), std::move(new_s3_settings),
        version_id, s3_capabilities, new_namespace,
        S3::URI(Poco::URI(config.getString(config_prefix + ".endpoint"))).endpoint);
}

}


#endif
