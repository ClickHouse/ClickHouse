#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
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
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>

#include <chrono>
#include <thread>
#include <unordered_set>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueMovedObjects;
    extern const Event ObjectStorageQueueMoveCollisions;
    extern const Event ObjectStorageQueueRemovedObjects;
    extern const Event ObjectStorageQueueTaggedObjects;
}

namespace DB
{

namespace FailPoints
{
    extern const char object_storage_queue_fail_delete[];
    extern const char object_storage_queue_fail_after_move_copy[];
}

#if USE_AWS_S3

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsString external_id;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString metadata_service;
    extern const S3AuthSettingsString request_token_path;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
}

#endif

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
    extern const int FILE_ALREADY_EXISTS;
}

namespace
{

/// Provenance lets a later attempt recognize a committed copy after an interrupted move.
constexpr auto move_source_path_attribute = "clickhouse_move_source_path";
constexpr auto move_source_etag_attribute = "clickhouse_move_source_etag";
constexpr auto move_source_last_modified_attribute = "clickhouse_move_source_last_modified";
constexpr auto move_source_version_id_attribute = "clickhouse_move_source_version_id";

std::optional<ObjectAttributes> makeMoveProvenance(
    ObjectAttributes source_attributes,
    const String & source_path,
    const String & source_etag,
    time_t source_last_modified,
    const String & source_version_id = {})
{
    if (source_etag.empty())
        return std::nullopt;
    source_attributes[move_source_path_attribute] = source_path;
    source_attributes[move_source_etag_attribute] = source_etag;
    source_attributes[move_source_last_modified_attribute] = toString(size_t(source_last_modified));
    if (!source_version_id.empty())
        source_attributes[move_source_version_id_attribute] = source_version_id;
    return source_attributes;
}

bool destinationIsOwnCommittedCopy(const std::optional<ObjectAttributes> & provenance, const ObjectAttributes & destination_attributes)
{
    if (!provenance)
        return false;
    for (const auto * key :
         {move_source_path_attribute, move_source_etag_attribute, move_source_last_modified_attribute})
    {
        auto expected = provenance->find(key);
        auto actual = destination_attributes.find(key);
        if (expected == provenance->end() || actual == destination_attributes.end() || actual->second != expected->second)
            return false;
    }
    /// Compared only when both sides carry one, so a destination stamped before this field existed
    /// still completes its interrupted move after an upgrade.
    auto expected_version = provenance->find(move_source_version_id_attribute);
    auto actual_version = destination_attributes.find(move_source_version_id_attribute);
    if (expected_version != provenance->end() && actual_version != destination_attributes.end()
        && actual_version->second != expected_version->second)
        return false;
    return true;
}

}

ObjectStorageQueuePostProcessor::ObjectStorageQueuePostProcessor(
    ContextPtr context_,
    ObjectStorageType type_,
    ObjectStoragePtr object_storage_,
    const ObjectStorageQueueTableMetadata & table_metadata_,
    AfterProcessingSettings settings_)
    : WithContext(context_)
    , type(type_)
    , object_storage(object_storage_)
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
                fiu_do_on(FailPoints::object_storage_queue_fail_delete, {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Failed to remove objects");
                });
                object_storage->removeObjectsIfExist(objects);
            });
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueRemovedObjects, objects.size());
        }
        catch (...)
        {
            LOG_WARNING(
                log,
                "Failed to remove all {} objects with exception: {}",
                objects.size(),
                getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
            );
        }
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
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTaggedObjects, objects.size());
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

bool ObjectStorageQueuePostProcessor::copyAndRemoveObject(const StoredObject & object, const std::function<bool()> & copy_object) const
{
    bool copy_finished = false;
    bool destination_is_ours = true;
    doWithRetries(
        [&]
        {
            if (!copy_finished)
            {
                destination_is_ours = copy_object();
                if (!destination_is_ours)
                    return;

                fiu_do_on(FailPoints::object_storage_queue_fail_after_move_copy, {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Failed after copying the object");
                });
                copy_finished = true;
            }

            LOG_INFO(log, "Removing object {}", object.remote_path);
            object_storage->removeObjectIfExists(object);
        });
    return destination_is_ours;
}

void ObjectStorageQueuePostProcessor::reportMoveCollision(const StoredObject & source, const StoredObject & destination) const
{
    LOG_ERROR(
        log,
        "Not moving object {}: destination object {} already exists; leaving the source in place "
        "(consider setting `after_processing_move_preserve_path`)",
        source.remote_path,
        destination.remote_path);
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
}

static StoredObject applyMovePrefixIfPresent(const StoredObject & src, const String & move_prefix, bool preserve_path)
{
    if (move_prefix.empty())
    {
        return src;
    }
    const String suffix = preserve_path ? src.remote_path : fileName(src.remote_path);
    chassert(!suffix.starts_with('/'));
    const String remote_path = fs::path(move_prefix) / suffix;
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

void ObjectStorageQueuePostProcessor::moveWithinBucket(const StoredObjects & objects, const String & move_prefix, bool preserve_path) const
{
    auto read_settings = getReadSettings();
    auto move_write_settings = getWriteSettings();

    /// Flattened moves need an atomic no-overwrite precondition.
    if (!preserve_path)
        move_write_settings.object_storage_write_if_none_match = "*";
    move_write_settings.object_storage_copy_preserve_source_tags = settings.after_processing_move_preserve_tags;

    auto schedule = threadPoolCallbackRunnerUnsafe<void>(
        IObjectStorage::getThreadPoolWriter(),
        ThreadName::REMOTE_FS_WRITE_THREAD_POOL);

    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);
    TaskTracker task_tracker(schedule, post_process_max_inflight_object_moves, limited_log);

    std::atomic<size_t> moved_objects = 0;
    std::unordered_set<String> destinations;

    try
    {
        for (const auto & object_from : objects)
        {
            auto destination = applyMovePrefixIfPresent(object_from, move_prefix, preserve_path);
            if (!destinations.insert(destination.remote_path).second)
            {
                reportMoveCollision(object_from, destination);
                continue;
            }
            /// The task outlives this iteration, so it takes its own copy of the source object.
            task_tracker.add(
                [&, source_object = object_from, object_to = std::move(destination)]
                {
                    try
                    {
                        auto copy_object = [&]
                        {
                            LOG_TRACE(log, "Copying object {} to {}", source_object.remote_path, object_to.remote_path);
                            std::optional<ObjectAttributes> provenance;
                            if (!preserve_path)
                            {
                                if (auto source_metadata
                                    = object_storage->tryGetObjectMetadata(source_object.remote_path, /*with_tags=*/false))
                                {
                                    provenance = makeMoveProvenance(
                                        source_metadata->attributes,
                                        source_object.remote_path,
                                        source_metadata->etag,
                                        source_metadata->last_modified.epochTime(),
                                        source_metadata->version_id);
                                }
                            }

                            try
                            {
                                object_storage->copyObject(source_object, object_to, read_settings, move_write_settings, provenance);
                            }
                            catch (const Exception & e)
                            {
                                if (e.code() != ErrorCodes::FILE_ALREADY_EXISTS)
                                    throw;

                                auto destination_metadata
                                    = object_storage->tryGetObjectMetadata(object_to.remote_path, /*with_tags=*/false);
                                return destination_metadata && destinationIsOwnCommittedCopy(provenance, destination_metadata->attributes);
                            }
                            return true;
                        };
                        if (!copyAndRemoveObject(source_object, copy_object))
                        {
                            reportMoveCollision(source_object, object_to);
                            return;
                        }
                        ++moved_objects;
                    }
                    catch (...)
                    {
                        LOG_WARNING(
                            log,
                            "Failed to move object {} within its storage with exception: {}",
                            source_object.remote_path,
                            getExceptionMessage(std::current_exception(), /*with_stacktrace=*/false));
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
            /// The move uses its own explicit keys, so drop every server-managed mechanism inherited from
            /// `<s3>` config (role_arn STS, GCP OAuth, and the server's temporary session_token) that would
            /// otherwise use the server's identity on top of those keys.
            s3_settings->auth_settings[S3AuthSetting::session_token] = "";
            s3_settings->auth_settings[S3AuthSetting::role_arn] = "";
            s3_settings->auth_settings[S3AuthSetting::role_session_name] = "";
            s3_settings->auth_settings[S3AuthSetting::external_id] = "";
            s3_settings->auth_settings[S3AuthSetting::http_client] = "";
            s3_settings->auth_settings[S3AuthSetting::service_account] = "";
            s3_settings->auth_settings[S3AuthSetting::metadata_service] = "";
            s3_settings->auth_settings[S3AuthSetting::request_token_path] = "";
            s3_settings->auth_settings[S3AuthSetting::google_adc_client_id] = "";
            s3_settings->auth_settings[S3AuthSetting::google_adc_client_secret] = "";
            s3_settings->auth_settings[S3AuthSetting::google_adc_refresh_token] = "";
            /// The move uses its own explicit keys, so also drop the request-auth material (headers/access
            /// headers and SSE-C/SSE-KMS keys) merged from the server `<s3>` config: otherwise the server's
            /// headers or encryption keys would be sent to the user-supplied move destination.
            s3_settings->auth_settings.clearServerManagedRequestAuth();
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
            /// Prefixless moves do not flatten paths and need no guard.
            const String move_if_none_match
                = (!move_prefix.empty() && !settings.after_processing_move_preserve_path) ? "*" : "";
            std::unordered_set<String> destinations;
            const String src_bucket = s3_storage->getObjectsNamespace();
            for (const auto & object_from : objects)
            {
                auto object_to = applyMovePrefixIfPresent(object_from, move_prefix, settings.after_processing_move_preserve_path);
                if (!destinations.insert(object_to.remote_path).second)
                {
                    reportMoveCollision(object_from, object_to);
                    continue;
                }
                try
                {
                    auto copy_object = [&]
                    {
                        auto source_info = S3::getObjectInfo(
                            *src_client,
                            src_bucket,
                            object_from.remote_path,
                            /*version_id=*/{},
                            /*with_metadata=*/true,
                            /*with_tags=*/false);
                        /// A guarded move re-uploads the object, so the tags are read explicitly rather than through
                        /// the `HeadObject` tag count, which restricted credentials do not get to see.
                        std::optional<ObjectAttributes> source_tags;
                        if (!move_if_none_match.empty() && settings.after_processing_move_preserve_tags)
                            source_tags = S3::getObjectTags(*src_client, src_bucket, object_from.remote_path);
                        const auto provenance = move_if_none_match.empty() ? std::optional<ObjectAttributes>{}
                                                                           : makeMoveProvenance(
                                                                                 source_info.metadata,
                                                                                 object_from.remote_path,
                                                                                 source_info.etag,
                                                                                 source_info.last_modification_time,
                                                                                 source_info.version_id);

                        LOG_INFO(log, "Copying {} ({} Bytes) to bucket {}", object_from.remote_path, source_info.size, dst_uri.bucket);
                        try
                        {
                            copyS3File(
                                src_client,
                                /*src_bucket=*/src_bucket,
                                /*src_key=*/object_from.remote_path,
                                /*src_size=*/source_info.size,
                                /*dest_s3_client=*/dst_client,
                                /*dest_bucket=*/dst_uri.bucket,
                                /*dest_key=*/object_to.remote_path,
                                /*settings=*/s3_settings->request_settings,
                                /*read_settings=*/read_settings_to_use,
                                BlobStorageLogWriter::create(object_storage->getDiskName()),
                                scheduler,
                                /*fallback_file_reader=*/[&] { return s3_storage->readObject(object_from, read_settings_to_use); },
                                /*object_metadata=*/provenance,
                                S3CopyFileSettings{
                                    .if_none_match = move_if_none_match,
                                    .source_headers = move_if_none_match.empty() ? std::optional<S3::ObjectHeaders>{}
                                                                                 : std::optional<S3::ObjectHeaders>{source_info.headers},
                                    .source_tags = std::move(source_tags)});
                        }
                        catch (const Exception & e)
                        {
                            if (e.code() != ErrorCodes::FILE_ALREADY_EXISTS)
                                throw;

                            const auto destination_info = S3::getObjectInfoIfExists(
                                *dst_client, dst_uri.bucket, object_to.remote_path, /*version_id=*/{}, /*with_metadata=*/true);
                            return destinationIsOwnCommittedCopy(provenance, destination_info.metadata);
                        }
                        return true;
                    };
                    if (!copyAndRemoveObject(object_from, copy_object))
                    {
                        reportMoveCollision(object_from, object_to);
                        continue;
                    }

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
        moveWithinBucket(objects, move_prefix, settings.after_processing_move_preserve_path);
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
            /// Prefixless moves do not flatten paths and need no guard.
            const String move_if_none_match
                = (!move_prefix.empty() && !settings.after_processing_move_preserve_path) ? "*" : "";
            std::unordered_set<String> destinations;
            auto request_settings = azure_storage->getSettings();
            const auto read_settings = azure_storage->patchSettings(getReadSettings());
            auto scheduler = threadPoolCallbackRunnerUnsafe<void>(IObjectStorage::getThreadPoolWriter(), ThreadName::AZURE_COPY_POOL);
            for (const auto & object_from : objects)
            {
                auto object_to = applyMovePrefixIfPresent(object_from, move_prefix, settings.after_processing_move_preserve_path);
                if (!destinations.insert(object_to.remote_path).second)
                {
                    reportMoveCollision(object_from, object_to);
                    continue;
                }
                try
                {
                    auto copy_object = [&]
                    {
                        auto blob_client = src_client->GetBlobClient(object_from.remote_path);
                        auto properties = blob_client.GetProperties().Value;
                        auto blob_size = properties.BlobSize;
                        const auto provenance = move_if_none_match.empty()
                            ? std::optional<ObjectAttributes>{}
                            : makeMoveProvenance(
                                  ObjectAttributes{properties.Metadata.begin(), properties.Metadata.end()},
                                  object_from.remote_path,
                                  properties.ETag.ToString(),
                                  std::chrono::system_clock::to_time_t(
                                      static_cast<std::chrono::system_clock::time_point>(properties.LastModified)));
                        LOG_INFO(log, "Copying {} ({} Bytes) to container {}", object_from.remote_path, blob_size, move_container);
                        try
                        {
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
                                provenance,
                                scheduler,
                                /* blob_storage_log */ {},
                                /* dest_if_none_match */ move_if_none_match);
                        }
                        catch (const Azure::Core::RequestFailedException & e)
                        {
                            if (!move_if_none_match.empty() && isAzureDestinationAlreadyExistsError(e))
                            {
                                auto destination_properties = dst_client->GetBlobClient(object_to.remote_path).GetProperties().Value;
                                return destinationIsOwnCommittedCopy(
                                    provenance,
                                    ObjectAttributes{destination_properties.Metadata.begin(), destination_properties.Metadata.end()});
                            }
                            throw;
                        }
                        return true;
                    };
                    if (!copyAndRemoveObject(object_from, copy_object))
                    {
                        reportMoveCollision(object_from, object_to);
                        continue;
                    }

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
        moveWithinBucket(objects, move_prefix, settings.after_processing_move_preserve_path);
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
