#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>


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
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/copyS3File.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>

#include <Common/getRandomASCIIString.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>


namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
    extern const Event DiskS3DeleteObjects;
    extern const Event DiskS3ListObjects;
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

    if (!response.IsSuccess() && (!if_exists || !S3::isNotFoundError(response.GetError().GetErrorType())))
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

bool S3ObjectStorage::exists(const StoredObject & object) const
{
    auto settings_ptr = s3_settings.get();
    return S3::objectExists(*client.get(), bucket, object.absolute_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
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

    S3::ListObjectsV2Request request;
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

    S3::ListObjectsV2Request request;
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
    S3::DeleteObjectRequest request;
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
            S3::DeleteObjectsRequest request;
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
    auto settings_ptr = s3_settings.get();
    auto object_info = S3::getObjectInfo(*client.get(), bucket, path, {}, settings_ptr->request_settings, /* with_metadata= */ true, /* for_disk_s3= */ true);

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.last_modified = object_info.last_modification_time;
    result.attributes = object_info.metadata;

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
        auto client_ptr = client.get();
        auto settings_ptr = s3_settings.get();
        auto size = S3::getObjectSize(*client_ptr, bucket, object_from.absolute_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
        auto scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "S3ObjStor_copy");
        copyS3File(client_ptr, bucket, object_from.absolute_path, 0, size, dest_s3->bucket, object_to.absolute_path,
                   settings_ptr->request_settings, object_to_attributes, scheduler, /* for_disk_s3= */ true);
    }
    else
    {
        IObjectStorage::copyObjectToAnotherObjectStorage(object_from, object_to, object_storage_to, object_to_attributes);
    }
}

void S3ObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from, const StoredObject & object_to, std::optional<ObjectAttributes> object_to_attributes)
{
    auto client_ptr = client.get();
    auto settings_ptr = s3_settings.get();
    auto size = S3::getObjectSize(*client_ptr, bucket, object_from.absolute_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
    auto scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "S3ObjStor_copy");
    copyS3File(client_ptr, bucket, object_from.absolute_path, 0, size, bucket, object_to.absolute_path,
               settings_ptr->request_settings, object_to_attributes, scheduler, /* for_disk_s3= */ true);
}

void S3ObjectStorage::setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_)
{
    s3_settings.set(std::move(s3_settings_));
}

void S3ObjectStorage::setNewClient(std::unique_ptr<S3::Client> && client_)
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
    const_cast<S3::Client &>(*client_ptr).DisableRequestProcessing();
}

void S3ObjectStorage::startup()
{
    auto client_ptr = client.get();

    /// Need to be enabled if it was disabled during shutdown() call.
    const_cast<S3::Client &>(*client_ptr).EnableRequestProcessing();
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
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    return std::make_unique<S3ObjectStorage>(
        std::move(new_client), std::move(new_s3_settings),
        version_id, s3_capabilities, new_namespace,
        endpoint);
}

}


#endif
