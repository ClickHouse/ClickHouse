#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/copyS3File.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>

#include <Common/ProfileEvents.h>
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

namespace CurrentMetrics
{
    extern const Metric ObjectStorageS3Threads;
    extern const Metric ObjectStorageS3ThreadsActive;
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

namespace
{

class S3IteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    S3IteratorAsync(
        const std::string & bucket,
        const std::string & path_prefix,
        std::shared_ptr<const S3::Client> client_,
        size_t max_list_size)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageS3Threads,
            CurrentMetrics::ObjectStorageS3ThreadsActive,
            "ListObjectS3")
        , client(client_)
    {
        request.SetBucket(bucket);
        request.SetPrefix(path_prefix);
        request.SetMaxKeys(static_cast<int>(max_list_size));
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);

        bool result = false;
        auto outcome = client->ListObjectsV2(request);
        /// Outcome failure will be handled on the caller side.
        if (outcome.IsSuccess())
        {
            auto objects = outcome.GetResult().GetContents();

            result = !objects.empty();

            for (const auto & object : objects)
                batch.emplace_back(object.GetKey(), ObjectMetadata{static_cast<uint64_t>(object.GetSize()), Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()), {}});

            if (result)
                request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

            return result;
        }

        throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                quoteString(request.GetBucket()), quoteString(request.GetPrefix()),
                backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
    }

    std::shared_ptr<const S3::Client> client;
    S3::ListObjectsV2Request request;
};

}

bool S3ObjectStorage::exists(const StoredObject & object) const
{
    auto settings_ptr = s3_settings.get();
    return S3::objectExists(*clients.get()->client, bucket, object.remote_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    ReadSettings disk_read_settings = patchSettings(read_settings);
    auto global_context = Context::getGlobalContextInstance();

    auto settings_ptr = s3_settings.get();

    auto read_buffer_creator =
        [this, settings_ptr, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromS3>(
            clients.get()->client,
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

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */false);

        }
        case RemoteFSReadMethod::threadpool:
        {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl), reader, disk_read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
        }
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
        clients.get()->client,
        bucket,
        object.remote_path,
        version_id,
        settings_ptr->request_settings,
        patchSettings(read_settings));
}

std::unique_ptr<WriteBufferFromFileBase> S3ObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode, // S3 doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
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

    auto clients_ = clients.get();
    return std::make_unique<WriteBufferFromS3>(
        clients_->client,
        clients_->client_with_long_timeout,
        bucket,
        object.remote_path,
        buf_size,
        settings_ptr->request_settings,
        attributes,
        std::move(scheduler),
        disk_write_settings);
}


ObjectStorageIteratorPtr S3ObjectStorage::iterate(const std::string & path_prefix) const
{
    auto settings_ptr = s3_settings.get();
    auto client_ptr = clients.get()->client;

    return std::make_shared<S3IteratorAsync>(bucket, path_prefix, client_ptr, settings_ptr->list_object_keys_size);
}

void S3ObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const
{
    auto settings_ptr = s3_settings.get();
    auto client_ptr = clients.get()->client;

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
            children.emplace_back(object.GetKey(), ObjectMetadata{static_cast<uint64_t>(object.GetSize()), Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()), {}});

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

void S3ObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    auto client_ptr = clients.get()->client;

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    ProfileEvents::increment(ProfileEvents::DiskS3DeleteObjects);
    S3::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(object.remote_path);
    auto outcome = client_ptr->DeleteObject(request);

    throwIfUnexpectedError(outcome, if_exists);

    LOG_TRACE(log, "Object with path {} was removed from S3", object.remote_path);
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
        auto client_ptr = clients.get()->client;
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
                obj.SetKey(objects[current_position].remote_path);
                current_chunk.push_back(obj);

                if (!keys.empty())
                    keys += ", ";
                keys += objects[current_position].remote_path;
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

std::optional<ObjectMetadata> S3ObjectStorage::tryGetObjectMetadata(const std::string & path) const
{
    auto settings_ptr = s3_settings.get();
    auto object_info = S3::getObjectInfo(*clients.get()->client, bucket, path, {}, settings_ptr->request_settings, /* with_metadata= */ true, /* for_disk_s3= */ true, /* throw_on_error= */ false);

    if (object_info.size == 0 && object_info.last_modification_time == 0 && object_info.metadata.empty())
        return {};

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.last_modified = object_info.last_modification_time;
    result.attributes = object_info.metadata;

    return result;
}

ObjectMetadata S3ObjectStorage::getObjectMetadata(const std::string & path) const
{
    auto settings_ptr = s3_settings.get();
    auto object_info = S3::getObjectInfo(*clients.get()->client, bucket, path, {}, settings_ptr->request_settings, /* with_metadata= */ true, /* for_disk_s3= */ true);

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
        auto client_ptr = clients.get()->client;
        auto settings_ptr = s3_settings.get();
        auto size = S3::getObjectSize(*client_ptr, bucket, object_from.remote_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
        auto scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "S3ObjStor_copy");
        copyS3File(client_ptr, bucket, object_from.remote_path, 0, size, dest_s3->bucket, object_to.remote_path,
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
    auto client_ptr = clients.get()->client;
    auto settings_ptr = s3_settings.get();
    auto size = S3::getObjectSize(*client_ptr, bucket, object_from.remote_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
    auto scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "S3ObjStor_copy");
    copyS3File(client_ptr, bucket, object_from.remote_path, 0, size, bucket, object_to.remote_path,
               settings_ptr->request_settings, object_to_attributes, scheduler, /* for_disk_s3= */ true);
}

void S3ObjectStorage::setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_)
{
    s3_settings.set(std::move(s3_settings_));
}

void S3ObjectStorage::shutdown()
{
    auto clients_ptr = clients.get();
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    const_cast<S3::Client &>(*clients_ptr->client).DisableRequestProcessing();
    const_cast<S3::Client &>(*clients_ptr->client_with_long_timeout).DisableRequestProcessing();
}

void S3ObjectStorage::startup()
{
    auto clients_ptr = clients.get();

    /// Need to be enabled if it was disabled during shutdown() call.
    const_cast<S3::Client &>(*clients_ptr->client).EnableRequestProcessing();
    const_cast<S3::Client &>(*clients_ptr->client_with_long_timeout).EnableRequestProcessing();
}

void S3ObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto new_s3_settings = getSettings(config, config_prefix, context);
    auto new_client = getClient(config, config_prefix, context, *new_s3_settings);
    auto new_clients = std::make_unique<Clients>(std::move(new_client), *new_s3_settings);
    s3_settings.set(std::move(new_s3_settings));
    clients.set(std::move(new_clients));
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

S3ObjectStorage::Clients::Clients(std::shared_ptr<S3::Client> client_, const S3ObjectStorageSettings & settings)
    : client(std::move(client_)), client_with_long_timeout(client->clone(std::nullopt, settings.request_settings.long_request_timeout_ms)) {}

}

#endif
