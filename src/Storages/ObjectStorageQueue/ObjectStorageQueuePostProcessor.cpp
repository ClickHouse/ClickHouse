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

/// True when a copy made conditional with `If-None-Match: *` was rejected because the destination
/// already exists. Every object storage reports that as FILE_ALREADY_EXISTS: the S3 helpers classify
/// the 412 where the raw error is still available, since S3Exception keeps only the S3Errors code.
bool isDestinationAlreadyExistsError(const Exception & e)
{
    return e.code() == ErrorCodes::FILE_ALREADY_EXISTS;
}

/// Provenance stamped onto the destination as object metadata: which source key and which content
/// version of it (by ETag and last-modification time) this copy was made from. Lowercase to survive
/// the S3 round-trip verbatim.
///
/// These identify a source version rather than a processing attempt on purpose: an attempt can die
/// between committing its copy and removing the source - server restart, task cancellation - so the
/// next attempt must be able to prove "this destination is my own earlier copy" without any memory
/// of its predecessor. A token regenerated per attempt could not, since nothing persists it; a
/// re-read of the current source does.
constexpr auto move_source_path_attribute = "clickhouse_move_source_path";
constexpr auto move_source_etag_attribute = "clickhouse_move_source_etag";
constexpr auto move_source_last_modified_attribute = "clickhouse_move_source_last_modified";

/// The copy replaces the destination's metadata wholesale, so the source's own user metadata is
/// carried along with the provenance keys rather than being dropped by the move. The times are
/// recorded as epoch-seconds strings: object metadata values are strings and a fixed rendering
/// keeps the later comparison exact.
std::optional<ObjectAttributes> makeMoveProvenance(
    ObjectAttributes source_attributes,
    const String & source_path,
    const String & source_etag,
    time_t source_last_modified)
{
    if (source_etag.empty())
        return std::nullopt;
    source_attributes[move_source_path_attribute] = source_path;
    source_attributes[move_source_etag_attribute] = source_etag;
    source_attributes[move_source_last_modified_attribute] = toString(size_t(source_last_modified));
    return source_attributes;
}

/// True when the "already existing" destination records the current content version of this source
/// (key, ETag, last-modification time) as its origin: our own earlier copy committed and only a
/// post-copy step failed - possibly across a restart that lost every attempt's in-memory state - so
/// removing the source loses nothing.
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
    return true;
}

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

/// With preserve_path = false, objects with equal basenames under different prefixes map to the
/// same destination; moving them all would silently overwrite data (see #114847).
static std::vector<UInt8> findDuplicateMoveDestinations(const StoredObjects & objects, const String & move_prefix, bool preserve_path)
{
    std::vector<UInt8> duplicate(objects.size(), 0);
    std::unordered_set<String> destinations;
    for (size_t i = 0; i < objects.size(); ++i)
        if (!destinations.insert(applyMovePrefixIfPresent(objects[i], move_prefix, preserve_path).remote_path).second)
            duplicate[i] = 1;
    return duplicate;
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

    /// With the path preserved, destinations are unique by construction. Without it, several source
    /// keys can flatten onto one destination, so the copy itself must refuse to overwrite - checking
    /// with a separate exists() call first would still let two concurrent movers both pass the check.
    if (!preserve_path)
        move_write_settings.object_storage_write_if_none_match = "*";
    move_write_settings.object_storage_copy_preserve_source_tags = settings.after_processing_move_preserve_tags;

    auto schedule = threadPoolCallbackRunnerUnsafe<void>(
        IObjectStorage::getThreadPoolWriter(),
        ThreadName::REMOTE_FS_WRITE_THREAD_POOL);

    LogSeriesLimiterPtr limited_log = std::make_shared<LogSeriesLimiter>(log, 1, 5);
    TaskTracker task_tracker(schedule, post_process_max_inflight_object_moves, limited_log);

    std::atomic<size_t> moved_objects = 0;
    const auto duplicate_destination = findDuplicateMoveDestinations(objects, move_prefix, preserve_path);

    try
    {
        for (size_t i = 0; i < objects.size(); ++i)
        {
            if (duplicate_destination[i])
            {
                LOG_ERROR(
                    log,
                    "Not moving object {}: its destination collides with another object's destination "
                    "(consider setting after_processing_move_preserve_path); leaving the object in place",
                    objects[i].remote_path);
                ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
                continue;
            }
            task_tracker.add([&, &object_from = objects[i]]{
                try
                {
                    auto object_to = applyMovePrefixIfPresent(object_from, move_prefix, preserve_path);

                    /// `copied` makes the retry idempotent: once the destination is written, a failure
                    /// of the removal below must not re-run the copy, which would then be rejected by
                    /// its own precondition and be mistaken for a collision.
                    bool copied = false;
                    bool destination_exists = false;
                    doWithRetries([&]{
                        if (!copied)
                        {
                            LOG_TRACE(log, "Copying object {} to {}", object_from.remote_path, object_to.remote_path);
                            /// Stamped onto the destination so a later attempt can prove "this is my own
                            /// earlier copy" on a rejected precondition; an unguarded copy has no use for
                            /// it and keeps the native copy's header/metadata preservation instead.
                            std::optional<ObjectAttributes> provenance;
                            if (!preserve_path)
                                if (auto source_metadata = object_storage->tryGetObjectMetadata(object_from.remote_path, /*with_tags=*/ false))
                                    provenance = makeMoveProvenance(
                                        source_metadata->attributes,
                                        object_from.remote_path,
                                        source_metadata->etag,
                                        source_metadata->last_modified.epochTime());
                            try
                            {
                                object_storage->copyObject(
                                    object_from,
                                    object_to,
                                    read_settings,
                                    move_write_settings,
                                    provenance);
                            }
                            catch (const Exception & e)
                            {
                                /// Losing the race is a final answer, not something to retry - unless the
                                /// destination records this source as its origin: an earlier attempt
                                /// committed the copy and failed only afterwards.
                                if (isDestinationAlreadyExistsError(e))
                                {
                                    auto destination_metadata = object_storage->tryGetObjectMetadata(object_to.remote_path, /*with_tags=*/ false);
                                    if (!destination_metadata
                                        || !destinationIsOwnCommittedCopy(provenance, destination_metadata->attributes))
                                    {
                                        destination_exists = true;
                                        return;
                                    }
                                }
                                else
                                    throw;
                            }
                            /// The destination is committed but the move is not finished: this is the
                            /// window a retry has to recognize as its own copy rather than a collision.
                            fiu_do_on(FailPoints::object_storage_queue_fail_after_move_copy, {
                                throw Exception(ErrorCodes::FAULT_INJECTED, "Failed after copying the object");
                            });
                            copied = true;
                        }
                        LOG_INFO(log, "Removing object {}", object_from.remote_path);
                        object_storage->removeObjectIfExists(object_from);
                    });

                    if (destination_exists)
                    {
                        LOG_ERROR(
                            log,
                            "Not moving object {} to {}: destination object already exists "
                            "(consider setting after_processing_move_preserve_path); leaving the object in place",
                            object_from.remote_path,
                            object_to.remote_path);
                        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
                        return;
                    }
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
            /// Without the path preserved, several source keys can flatten onto one destination, so the
            /// copy itself must refuse to overwrite; a separate objectExists() check would still let two
            /// concurrent movers both pass it. With no prefix nothing is flattened - the destination key
            /// is the source key - so guarding there would only demand tag-read rights the plain move
            /// never needed.
            const String move_if_none_match
                = (!move_prefix.empty() && !settings.after_processing_move_preserve_path) ? "*" : "";
            const auto duplicate_destination = findDuplicateMoveDestinations(objects, move_prefix, settings.after_processing_move_preserve_path);
            for (size_t i = 0; i < objects.size(); ++i)
            {
                const auto & object_from = objects[i];
                if (duplicate_destination[i])
                {
                    LOG_ERROR(
                        log,
                        "Not moving object {}: its destination collides with another object's destination "
                        "(consider setting after_processing_move_preserve_path); leaving the object in place",
                        object_from.remote_path);
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
                    continue;
                }
                try
                {
                    auto object_to = applyMovePrefixIfPresent(object_from, move_prefix, settings.after_processing_move_preserve_path);

                    /// See moveWithinBucket(): the copy has to refuse an existing destination itself,
                    /// and the retry must not re-run it once it has succeeded.
                    bool copied = false;
                    bool destination_exists = false;
                    doWithRetries([&]{
                        if (!copied)
                        {
                            const String src_bucket = s3_storage->getObjectsNamespace();
                            const auto source_info = S3::getObjectInfo(
                                *src_client,
                                src_bucket,
                                object_from.remote_path,
                                /*version_id=*/ {},
                                /*with_metadata=*/ true,
                                /*with_tags=*/ !move_if_none_match.empty() && settings.after_processing_move_preserve_tags);
                            /// See moveWithinBucket(): lets a later attempt recognize its own committed
                            /// copy; an unguarded copy keeps the native header/metadata preservation instead.
                            const auto provenance = move_if_none_match.empty()
                                ? std::optional<ObjectAttributes>{}
                                : makeMoveProvenance(
                                    source_info.metadata,
                                    object_from.remote_path,
                                    source_info.etag,
                                    source_info.last_modification_time);

                            LOG_INFO(log, "Copying {} ({} Bytes) to bucket {}", object_from.remote_path, source_info.size, dst_uri.bucket);
                            try
                            {
                                copyS3File(
                                    src_client,
                                    /*src_bucket=*/ src_bucket,
                                    /*src_key=*/ object_from.remote_path,
                                    /*src_size=*/ source_info.size,
                                    /*dest_s3_client=*/ dst_client,
                                    /*dest_bucket=*/ dst_uri.bucket,
                                    /*dest_key=*/ object_to.remote_path,
                                    /*settings=*/ s3_settings->request_settings,
                                    /*read_settings=*/ read_settings_to_use,
                                    BlobStorageLogWriter::create(object_storage->getDiskName()),
                                    scheduler,
                                    /*fallback_file_reader=*/ [&]{
                                        return s3_storage->readObject(object_from, read_settings_to_use);
                                    },
                                    /*object_metadata=*/ provenance,
                                    /*dest_if_none_match=*/ move_if_none_match,
                                    /// The guard keeps this copy off CopyObject, so the headers and
                                    /// tags it would have carried over have to be restated on the upload.
                                    /*source_headers=*/ move_if_none_match.empty()
                                        ? std::optional<S3::ObjectHeaders>{}
                                        : std::optional<S3::ObjectHeaders>{source_info.headers},
                                    /*source_tags=*/ move_if_none_match.empty()
                                        ? std::optional<ObjectAttributes>{}
                                        : std::optional<ObjectAttributes>{source_info.tags});
                            }
                            catch (const Exception & e)
                            {
                                /// See moveWithinBucket(): a destination recording this source as its origin
                                /// means an earlier attempt committed the copy and failed only afterwards.
                                if (isDestinationAlreadyExistsError(e))
                                {
                                    const auto destination_info = S3::getObjectInfoIfExists(
                                        *dst_client, dst_uri.bucket, object_to.remote_path, /*version_id=*/ {}, /*with_metadata=*/ true);
                                    if (!destinationIsOwnCommittedCopy(provenance, destination_info.metadata))
                                    {
                                        destination_exists = true;
                                        return;
                                    }
                                }
                                else
                                    throw;
                            }
                            /// The destination is committed but the move is not finished: this is the
                            /// window a retry has to recognize as its own copy rather than a collision.
                            fiu_do_on(FailPoints::object_storage_queue_fail_after_move_copy, {
                                throw Exception(ErrorCodes::FAULT_INJECTED, "Failed after copying the object");
                            });
                            copied = true;
                        }

                        LOG_INFO(log, "Removing object {}", object_from.remote_path);
                        object_storage->removeObjectIfExists(object_from);
                    });

                    if (destination_exists)
                    {
                        LOG_ERROR(
                            log,
                            "Not moving object {} to bucket {}: destination object {} already exists "
                            "(consider setting after_processing_move_preserve_path); leaving the object in place",
                            object_from.remote_path,
                            dst_uri.bucket,
                            object_to.remote_path);
                        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
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
            /// See moveS3Objects(): flattened destinations can collide across batches and movers, so the
            /// copy itself must refuse to overwrite rather than be preceded by a separate check, and a
            /// prefixless move flattens nothing and stays unguarded.
            const String move_if_none_match
                = (!move_prefix.empty() && !settings.after_processing_move_preserve_path) ? "*" : "";
            const auto duplicate_destination = findDuplicateMoveDestinations(objects, move_prefix, settings.after_processing_move_preserve_path);
            for (size_t i = 0; i < objects.size(); ++i)
            {
                const auto & object_from = objects[i];
                if (duplicate_destination[i])
                {
                    LOG_ERROR(
                        log,
                        "Not moving object {}: its destination collides with another object's destination "
                        "(consider setting after_processing_move_preserve_path); leaving the object in place",
                        object_from.remote_path);
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
                    continue;
                }
                try
                {
                    auto object_to = applyMovePrefixIfPresent(object_from, move_prefix, settings.after_processing_move_preserve_path);

                    /// See moveWithinBucket(): the copy has to refuse an existing destination itself,
                    /// and the retry must not re-run it once it has succeeded.
                    bool copied = false;
                    bool destination_exists = false;
                    doWithRetries([&]{
                        if (!copied)
                        {
                            Azure::Storage::Blobs::BlobClient blobClient = src_client->GetBlobClient(object_from.remote_path);
                            auto properties = blobClient.GetProperties().Value;
                            auto blob_size = properties.BlobSize;
                            /// See moveWithinBucket(): lets a later attempt recognize its own committed
                            /// copy; an unguarded copy leaves the destination's user metadata untouched.
                            const auto provenance = move_if_none_match.empty()
                                ? std::optional<ObjectAttributes>{}
                                : makeMoveProvenance(
                                    ObjectAttributes{properties.Metadata.begin(), properties.Metadata.end()},
                                    object_from.remote_path,
                                    properties.ETag.ToString(),
                                    std::chrono::system_clock::to_time_t(static_cast<std::chrono::system_clock::time_point>(properties.LastModified)));
                            auto request_settings = azure_storage->getSettings();
                            auto read_settings = getReadSettings();
                            const auto read_settings_to_use = azure_storage->patchSettings(read_settings);
                            auto scheduler = threadPoolCallbackRunnerUnsafe<void>(
                                IObjectStorage::getThreadPoolWriter(),
                                ThreadName::AZURE_COPY_POOL);

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
                                    /* dest_if_none_match */ move_if_none_match
                                );
                            }
                            catch (const Azure::Core::RequestFailedException & e)
                            {
                                /// Azure answers 409 BlobAlreadyExists / 412 for a rejected If-None-Match.
                                /// Without the precondition (path-preserving moves) such a status is no
                                /// evidence of a collision, so it is not swallowed - mirroring
                                /// AzureObjectStorage::copyObject.
                                if (!move_if_none_match.empty()
                                    && (e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                                        || e.StatusCode == Azure::Core::Http::HttpStatusCode::PreconditionFailed))
                                {
                                    /// See moveWithinBucket(): a destination recording this source as its
                                    /// origin means an earlier attempt committed the copy. A failing lookup
                                    /// escapes into the retry loop: swallowed as a collision it would strand
                                    /// a source whose copy may well have committed.
                                    auto destination_properties = dst_client->GetBlobClient(object_to.remote_path).GetProperties().Value;
                                    bool own_committed_copy = destinationIsOwnCommittedCopy(
                                        provenance,
                                        ObjectAttributes{destination_properties.Metadata.begin(), destination_properties.Metadata.end()});
                                    if (!own_committed_copy)
                                    {
                                        destination_exists = true;
                                        return;
                                    }
                                }
                                else
                                    throw;
                            }
                            /// The destination is committed but the move is not finished: this is the
                            /// window a retry has to recognize as its own copy rather than a collision.
                            fiu_do_on(FailPoints::object_storage_queue_fail_after_move_copy, {
                                throw Exception(ErrorCodes::FAULT_INJECTED, "Failed after copying the object");
                            });
                            copied = true;
                        }

                        LOG_INFO(log, "Removing object {}", object_from.remote_path);
                        object_storage->removeObjectIfExists(object_from);
                    });

                    if (destination_exists)
                    {
                        LOG_ERROR(
                            log,
                            "Not moving object {} to container {}: destination object {} already exists "
                            "(consider setting after_processing_move_preserve_path); leaving the object in place",
                            object_from.remote_path,
                            move_container,
                            object_to.remote_path);
                        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveCollisions);
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
