#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <IO/ReadSettings.h>
#include <Common/BlobStorageLogWriter.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>

#include <chrono>
#include <thread>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueMovedObjects;
    extern const Event ObjectStorageQueueRemovedObjects;
    extern const Event ObjectStorageQueueTaggedObjects;
}

namespace DB
{

#if USE_AWS_S3

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
}

#endif

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

ObjectStorageQueuePostProcessor::ObjectStorageQueuePostProcessor(
    ContextPtr context_,
    ObjectStorageType type_,
    ObjectStoragePtr object_storage_,
    String engine_name_,
    const ObjectStorageQueueTableMetadata & table_metadata_,
    AfterProcessingSettings settings_)
    : WithContext(context_)
    , type(type_)
    , object_storage(object_storage_)
    , engine_name(engine_name_)
    , table_metadata(table_metadata_)
    , settings(std::move(settings_))
    , log(getLogger("ObjectStorageQueuePostProcessor"))
{ }

void ObjectStorageQueuePostProcessor::process(const StoredObjects & objects) const
{
    const ObjectStorageQueueAction after_processing_action = table_metadata.after_processing.load();
    if (after_processing_action == ObjectStorageQueueAction::DELETE)
    {
        LOG_TRACE(log, "Removing {} objects", objects.size());

        /// We do need to apply after-processing action before committing requests to keeper.
        /// See explanation in ObjectStorageQueueSource::FileIterator::nextImpl().
        try
        {
            doWithRetries([&]{
                object_storage->removeObjectsIfExist(objects);
            });
        }
        catch (...)
        {
            LOG_WARNING(
                log,
                "Failed to tag all {} objects with exception: {}",
                objects.size(),
                getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
            );
        }
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueRemovedObjects, objects.size());
    }
    else if (after_processing_action == ObjectStorageQueueAction::MOVE)
    {
        switch (type)
        {
            case ObjectStorageType::Azure:
                moveAzureBlobs(objects);
                break;
            case ObjectStorageType::S3:
                moveS3Objects(objects);
                break;
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "After processing move not allowed for storage type {}, only Azure and S3 supported",
                    type);
        }
    }
    else if (after_processing_action == ObjectStorageQueueAction::TAG)
    {
#if USE_AWS_S3 || USE_AZURE_BLOB_STORAGE
        const String & tag_key = settings.after_processing_tag_key;
        const String & tag_value = settings.after_processing_tag_value;
        LOG_INFO(log, "Executing TAG action in ObjectStorage Queue commit stage, {} = {}", tag_key, tag_value);
        try
        {
            doWithRetries([&]{
                object_storage->tagObjects(objects, tag_key, tag_value);
            });
        }
        catch (...)
        {
            LOG_WARNING(
                log,
                "Failed to tag all {} objects with exception: {}",
                objects.size(),
                getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
            );
        }
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTaggedObjects, objects.size());
#else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unsupported after_processing action for object storage type {}",
            type);
#endif
    }
    else if (after_processing_action != ObjectStorageQueueAction::KEEP)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unsupported after_processing action {}",
            ObjectStorageQueueTableMetadata::actionToString(after_processing_action));
    }

}

static constexpr size_t post_process_initial_backoff_ms = 100;
static constexpr size_t post_process_max_backoff_ms = 5000;
static constexpr size_t post_process_max_inflight_object_moves = 20;

void ObjectStorageQueuePostProcessor::doWithRetries(std::function<void()> action) const
{
    size_t backoff_ms = post_process_initial_backoff_ms;
    size_t retries = settings.after_processing_retries;

    for (size_t try_no = 0; try_no <= retries; ++try_no)
    {
        try
        {
            action();
            break;
        }
        catch (...)
        {
            LOG_DEBUG(
                log,
                "Action attempt #{} out of {} failed with exception: {}",
                try_no + 1,
                retries + 1,
                getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
            );
            if (try_no >= retries)
            {
                // Letting the caller to catch the exception and log it with a meaningful message
                throw;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(backoff_ms * 2, post_process_max_backoff_ms);
        }
    }
}

static StoredObject applyMovePrefixIfPresent(const StoredObject & src, const String & move_prefix)
{
    if (move_prefix.empty())
    {
        return src;
    }
    const String file_name = fileName(src.remote_path);
    const String remote_path = fs::path(move_prefix) / file_name;
    return StoredObject(remote_path);
}

#if USE_AZURE_BLOB_STORAGE

static AzureBlobStorage::ConnectionParams getAzureConnectionParams(
    const String & connection_url,
    const String & container_name,
    const ContextPtr & local_context)
{
    AzureBlobStorage::ConnectionParams connection_params;
    auto request_settings = AzureBlobStorage::getRequestSettings(local_context->getSettingsRef());

    AzureBlobStorage::processURL(connection_url, container_name, connection_params.endpoint, connection_params.auth_method);
    connection_params.client_options = AzureBlobStorage::getClientOptions(local_context, local_context->getSettingsRef(), *request_settings, /*for_disk=*/ false);

    return connection_params;
}

#endif

void ObjectStorageQueuePostProcessor::moveWithinBucket(const StoredObjects & objects, const String & move_prefix) const
{
    auto read_settings = getReadSettings();
    auto write_settings = getWriteSettings();

    auto schedule = threadPoolCallbackRunnerUnsafe<void>(
        IObjectStorage::getThreadPoolWriter(),
        ThreadName::REMOTE_FS_WRITE_THREAD_POOL);

    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);
    TaskTracker task_tracker(schedule, post_process_max_inflight_object_moves, limited_log);

    std::atomic<size_t> moved_objects = 0;

    try
    {
        for (const auto & object_from : objects)
        {
            task_tracker.add([&]{
                try
                {
                    doWithRetries([&]{
                        auto object_to = applyMovePrefixIfPresent(object_from, move_prefix);
                        LOG_TRACE(log, "Copying object {} to {}", object_from.remote_path, object_to.remote_path);
                        object_storage->copyObject(
                            object_from,
                            object_to,
                            read_settings,
                            write_settings);
                        LOG_INFO(log, "Removing object {}", object_from.remote_path);
                        object_storage->removeObjectIfExists(object_from);
                    });
                    ++moved_objects;
                }
                catch (...)
                {
                    LOG_WARNING(
                        log,
                        "Failed to move S3 object {} within bucket with exception: {}",
                        object_from.remote_path,
                        getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
                    );
                }
            });
        }
        task_tracker.waitAll();
    }
    catch (...)
    {
        LOG_WARNING(
            log,
            "Exception while moving objects to prefix {}: {}",
            move_prefix,
            getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
        );

        task_tracker.safeWaitAll();

        throw;
    }
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, moved_objects);
}

void ObjectStorageQueuePostProcessor::moveS3Objects(const StoredObjects & objects) const
{
#if USE_AWS_S3
    const String & move_uri = settings.after_processing_move_uri;
    const String & move_access_key_id = settings.after_processing_move_access_key_id;
    const String & move_secret_access_key = settings.after_processing_move_secret_access_key;
    const String & move_prefix = settings.after_processing_move_prefix;

    if (!move_uri.empty() || !move_access_key_id.empty() || !move_secret_access_key.empty())
    {
        if (move_uri.empty() || move_access_key_id.empty() || move_secret_access_key.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not enough settings to move S3 objects");
        }

        if (auto * s3_storage = dynamic_cast<S3ObjectStorage * >(object_storage.get()); s3_storage != nullptr)
        {
            auto src_client = s3_storage->getS3StorageClient();
            auto s3_settings = std::make_unique<S3Settings>();
            auto contextPtr = getContext();
            s3_settings->loadFromConfig(
                contextPtr->getConfigRef(),
                /* config_prefix */ "s3",
                contextPtr->getSettingsRef()
            );
            s3_settings->auth_settings[S3AuthSetting::access_key_id] = move_access_key_id;
            s3_settings->auth_settings[S3AuthSetting::secret_access_key] = move_secret_access_key;
            std::shared_ptr<S3::Client> dst_client = getClient(
                move_uri,
                *s3_settings,
                contextPtr,
                /* for_disk_s3 */ false
            );
            auto dst_uri = S3::URI(move_uri);
            auto read_settings = getReadSettings();
            const auto read_settings_to_use = s3_storage->patchSettings(read_settings);
            auto scheduler = threadPoolCallbackRunnerUnsafe<void>(
                IObjectStorage::getThreadPoolWriter(),
                ThreadName::S3_COPY_POOL);

            size_t moved_objects = 0;
            for (const auto & object_from : objects)
            {
                try
                {
                    doWithRetries([&]{
                        const String src_bucket = s3_storage->getObjectsNamespace();
                        size_t object_size = S3::getObjectSize(
                            *src_client,
                            src_bucket,
                            object_from.remote_path);
                        auto object_to = applyMovePrefixIfPresent(object_from, move_prefix);

                        LOG_INFO(log, "Copying {} ({} Bytes) to bucket {}", object_from.remote_path, object_size, dst_uri.bucket);
                        copyS3File(
                            src_client,
                            /*src_bucket=*/ src_bucket,
                            /*src_key=*/ object_from.remote_path,
                            /*src_offset=*/ 0,
                            /*src_size=*/ object_size,
                            /*dest_s3_client=*/ dst_client,
                            /*dest_bucket=*/ dst_uri.bucket,
                            /*dest_key=*/ object_to.remote_path,
                            /*settings=*/ s3_settings->request_settings,
                            /*read_settings=*/ read_settings_to_use,
                            BlobStorageLogWriter::create(object_storage->getDiskName()),
                            scheduler,
                            /*fallback_file_reader=*/ [&]{
                                return s3_storage->readObject(object_from, read_settings_to_use);
                            }
                            /*object_metadata=*/);

                        LOG_INFO(log, "Removing object {}", object_from.remote_path);
                        object_storage->removeObjectIfExists(object_from);
                    });
                    moved_objects += 1;
                }
                catch (...)
                {
                    LOG_WARNING(
                        log,
                        "Failed to move S3 object {} with exception: {}",
                        object_from.remote_path,
                        getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
                    );
                }
            }
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, moved_objects);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Underlying storage is not S3");
        }
    }
    else if (!move_prefix.empty())
    {
        moveWithinBucket(objects, move_prefix);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No settings to move S3 objects");
    }
#else
    UNUSED(objects);
#endif
}

void ObjectStorageQueuePostProcessor::moveAzureBlobs(const StoredObjects & objects) const
{
#if USE_AZURE_BLOB_STORAGE
    const String & move_connection_string = settings.after_processing_move_connection_string;
    const String & move_container = settings.after_processing_move_container;
    const String & move_prefix = settings.after_processing_move_prefix;

    if (!move_connection_string.empty() || !move_container.empty())
    {
        if (move_connection_string.empty() || move_container.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not enough settings to move Azure blobs");
        }

        if (auto * azure_storage = dynamic_cast<AzureObjectStorage * >(object_storage.get()); azure_storage != nullptr)
        {
            auto contextPtr = getContext();
            std::shared_ptr<const AzureBlobStorage::ContainerClient> src_client = azure_storage->getAzureBlobStorageClient();
            auto connection_params = getAzureConnectionParams(
                move_connection_string,
                move_container,
                contextPtr);
            const bool is_readonly = true;
            std::shared_ptr<AzureBlobStorage::ContainerClient> dst_client = AzureBlobStorage::getContainerClient(
                connection_params,
                is_readonly);

            size_t moved_objects = 0;
            for (const auto & object_from : objects)
            {
                try
                {
                    doWithRetries([&]{
                        Azure::Storage::Blobs::BlobClient blobClient = src_client->GetBlobClient(object_from.remote_path);
                        auto properties = blobClient.GetProperties().Value;
                        auto blob_size = properties.BlobSize;
                        auto object_to = applyMovePrefixIfPresent(object_from, move_prefix);
                        auto request_settings = azure_storage->getSettings();
                        auto read_settings = getReadSettings();
                        const auto read_settings_to_use = azure_storage->patchSettings(read_settings);
                        auto scheduler = threadPoolCallbackRunnerUnsafe<void>(
                            IObjectStorage::getThreadPoolWriter(),
                            ThreadName::AZURE_COPY_POOL);

                        LOG_INFO(log, "Copying {} ({} Bytes) to container {}", object_from.remote_path, blob_size, move_container);
                        copyAzureBlobStorageFile(
                            src_client,
                            dst_client,
                            connection_params.getContainer(),
                            /* src_blob */ object_from.remote_path,
                            /* src_offset */ 0,
                            blob_size,
                            move_container,
                            /* dest_blob */ object_to.remote_path,
                            request_settings,
                            read_settings,
                            std::optional<ObjectAttributes>(),
                            scheduler
                        );
                        LOG_INFO(log, "Removing object {}", object_from.remote_path);
                        object_storage->removeObjectIfExists(object_from);
                    });
                    moved_objects += 1;
                }
                catch (...)
                {
                    LOG_WARNING(
                        log,
                        "Failed to move Azure object {} with exception: {}",
                        object_from.remote_path,
                        getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
                    );
                }
            }
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, moved_objects);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Underlying storage is not Azure");
        }
    }
    else if (!move_prefix.empty())
    {
        moveWithinBucket(objects, move_prefix);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No settings to move Azure blobs");
    }
#else
    UNUSED(objects);
#endif
}

}
