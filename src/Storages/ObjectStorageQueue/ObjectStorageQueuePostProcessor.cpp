#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/setThreadName.h>
#include <Common/SipHash.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadSettings.h>
#include <Common/BlobStorageLogWriter.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>
#include <base/scope_guard.h>

#include <chrono>
#include <thread>
#include <unordered_set>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueMovedObjects;
    extern const Event ObjectStorageQueueMoveCollisions;
    extern const Event ObjectStorageQueueMoveSourceRewritten;
    extern const Event ObjectStorageQueueRemovedObjects;
    extern const Event ObjectStorageQueueTaggedObjects;
}

namespace DB
{

namespace FailPoints
{
    extern const char object_storage_queue_fail_delete[];
    extern const char object_storage_queue_fail_after_move_copy[];
    extern const char object_storage_queue_pause_after_move_copy[];
    extern const char object_storage_queue_pause_after_move_source_lookup[];
    extern const char object_storage_queue_pause_before_post_process[];
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
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_CHANGED_DURING_READ;
    extern const int S3_ERROR;
    extern const int S3_OBJECT_CHANGED_DURING_READ;
}

namespace
{

/// Provenance lets a later attempt recognize a committed copy after an interrupted move.
constexpr auto move_source_path_attribute = "clickhouse_move_source_path";
constexpr auto move_source_etag_attribute = "clickhouse_move_source_etag";
constexpr auto move_source_last_modified_attribute = "clickhouse_move_source_last_modified";
constexpr auto move_source_version_id_attribute = "clickhouse_move_source_version_id";
/// The source generation alone does not prove who copied it: anything a `HeadObject` of the source shows
/// can be restamped onto other bytes. The token is a digest of that generation keyed by the queue's Keeper
/// identity, so every attempt of the same queue (after a restart, on another replica) stamps the same token,
/// while another queue moving the same key, or a restamp from public data, does not. The identity carries the
/// Keeper name as well as the path: one path under two Keeper names is two queues, not one.
constexpr auto move_token_attribute = "clickhouse_move_token";

String makeMoveToken(
    const String & keeper_identity,
    const String & source_path,
    const String & source_etag,
    time_t source_last_modified,
    const String & source_version_id)
{
    SipHash hash;
    hash.update(keeper_identity);
    hash.update(source_path);
    hash.update(source_etag);
    hash.update(source_last_modified);
    hash.update(source_version_id);
    return getSipHash128AsHexString(hash);
}

std::optional<ObjectAttributes> makeMoveProvenance(
    ObjectAttributes source_attributes,
    const String & keeper_identity,
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
    source_attributes[move_token_attribute]
        = makeMoveToken(keeper_identity, source_path, source_etag, source_last_modified, source_version_id);
    if (!source_version_id.empty())
        source_attributes[move_source_version_id_attribute] = source_version_id;
    return source_attributes;
}

bool destinationIsOwnCommittedCopy(const std::optional<ObjectAttributes> & provenance, const ObjectAttributes & destination_attributes)
{
    if (!provenance)
        return false;
    /// The token digests the source version id too, so a destination stamped for another generation
    /// of the same key does not match one built for this generation.
    for (const auto * key :
         {move_source_path_attribute, move_source_etag_attribute, move_source_last_modified_attribute, move_token_attribute})
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
    const ObjectStorageQueueTableMetadata & table_metadata_,
    AfterProcessingSettings settings_,
    String keeper_identity_)
    : WithContext(context_)
    , type(type_)
    , object_storage(object_storage_)
    , table_metadata(table_metadata_)
    , settings(std::move(settings_))
    , keeper_identity(std::move(keeper_identity_))
    , log(getLogger("ObjectStorageQueuePostProcessor"))
{ }

void ObjectStorageQueuePostProcessor::ChangedGeneration::rememberIfCurrentExceptionIsOne()
{
    /// The Azure paths report a replaced generation as `FILE_CHANGED_DURING_READ`; the S3 copy and
    /// the S3 read buffer report it as `S3_OBJECT_CHANGED_DURING_READ`. Both mean the same thing here.
    const int code = getCurrentExceptionCode();
    if (code != ErrorCodes::FILE_CHANGED_DURING_READ && code != ErrorCodes::S3_OBJECT_CHANGED_DURING_READ)
        return;

    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMoveSourceRewritten);
    std::lock_guard lock(mutex);
    if (!exception)
        exception = std::current_exception();
}

void ObjectStorageQueuePostProcessor::ChangedGeneration::rethrowIfAny() const
{
    std::lock_guard lock(mutex);
    if (exception)
        std::rethrow_exception(exception);
}

void ObjectStorageQueuePostProcessor::process(
    const StoredObjects & objects,
    UnorderedSetWithMemoryTracking<String> & failed_object_paths) const
{
    StoredObjects successful_objects;

    SCOPE_EXIT({
        UnorderedSetWithMemoryTracking<std::string_view> successful_paths;
        successful_paths.reserve(successful_objects.size());
        for (const auto & object : successful_objects)
            successful_paths.insert(object.remote_path);

        for (const auto & object : objects)
            if (!successful_paths.contains(object.remote_path))
                failed_object_paths.insert(object.remote_path);
    });

    /// Park between the read and the after-processing action. No-op unless explicitly enabled.
    FailPointInjection::pauseFailPoint(FailPoints::object_storage_queue_pause_before_post_process);

    const ObjectStorageQueueAction after_processing_action = table_metadata.after_processing.load();
    if (after_processing_action == ObjectStorageQueueAction::DELETE)
    {
        LOG_TRACE(log, "Removing {} objects", objects.size());

        /// On Azure and on S3 the delete is pinned to the ingested generation: `removeObjectsIfExist`
        /// sends the `ETag` of every object as `If-Match` (the `ETag` element of a `DeleteObjects`
        /// request on S3), so an object overwritten after it was read is left in place
        /// (`FILE_CHANGED_DURING_READ`) rather than deleted without the newer generation ever
        /// having been ingested. An untagged object would be deleted by path; the source never
        /// hands one over (it fails such a file instead of reading it).
        if (type == ObjectStorageType::Azure || type == ObjectStorageType::S3)
        {
            for (const auto & object : objects)
            {
                if (object.etag.empty())
                    throw Exception(
                        type == ObjectStorageType::Azure ? ErrorCodes::AZURE_BLOB_STORAGE_ERROR : ErrorCodes::S3_ERROR,
                        "Cannot delete {} object {}: the generation that was ingested is not known",
                        type, object.remote_path);
            }
        }

        /// We do need to apply after-processing action before committing requests to keeper.
        /// See explanation in ObjectStorageQueueSource::FileIterator::nextImpl().
        ChangedGeneration changed_generation;
        try
        {
            doWithRetries([&]{
                fiu_do_on(FailPoints::object_storage_queue_fail_delete, {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Failed to remove objects");
                });
                /// Two generations of one key can share an `ETag`, so on a versioned bucket `If-Match`
                /// alone would let a same-byte re-upload be deleted in place of the generation that was
                /// ingested. The version names it, and only a `HEAD` reports one: a listing never does.
                if (type == ObjectStorageType::S3 && deleteVersionedS3Objects(objects, successful_objects))
                    return;
                /// Deletes every object it can before reporting one that changed.
                object_storage->removeObjectsIfExist(objects, &successful_objects);
            });
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueRemovedObjects, objects.size());
        }
        catch (...)
        {
            changed_generation.rememberIfCurrentExceptionIsOne();
            LOG_WARNING(
                log,
                "Failed to remove all {} objects with exception: {}",
                objects.size(),
                getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
            );
        }
        changed_generation.rethrowIfAny();
    }
    else if (after_processing_action == ObjectStorageQueueAction::MOVE)
    {
        switch (type)
        {
            case ObjectStorageType::Azure:
                moveAzureBlobs(objects, successful_objects);
                break;
            case ObjectStorageType::S3:
                moveS3Objects(objects, successful_objects);
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
                object_storage->tagObjects(objects, tag_key, tag_value, &successful_objects);
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
            /// The object is no longer the generation that was ingested. That does not heal with
            /// time: every retry would be pinned to the same, now gone, generation and be refused
            /// again, so the object is left in place for the caller to report. A later retry that
            /// failed for another reason would also hide the change behind that other error, and
            /// in non-`EXCLUSIVE` mode that other error is only logged.
            const int code = getCurrentExceptionCode();
            if (code == ErrorCodes::FILE_CHANGED_DURING_READ || code == ErrorCodes::S3_OBJECT_CHANGED_DURING_READ)
                throw;
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

ObjectStorageQueuePostProcessor::MoveResult ObjectStorageQueuePostProcessor::copyAndRemoveObject(
    const StoredObject & object, const std::function<CopyResult()> & copy_object) const
{
    bool copy_finished = false;
    CopyResult copy_result;
    doWithRetries(
        [&]
        {
            if (!copy_finished)
            {
                copy_result = copy_object();
                if (!copy_result.destination_is_ours)
                    return;

                fiu_do_on(FailPoints::object_storage_queue_fail_after_move_copy, {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Failed after copying the object");
                });
                copy_finished = true;
                /// Park between the copy and the delete. No-op unless explicitly enabled.
                FailPointInjection::pauseFailPoint(FailPoints::object_storage_queue_pause_after_move_copy);
            }

            LOG_INFO(log, "Removing object {}", object.remote_path);
            removeCopiedSource(object, copy_result.consumed);
        });
    if (!copy_result.destination_is_ours)
        return MoveResult::DestinationCollision;
    return MoveResult::Moved;
}

/// `consumed` names a version only on S3, so a build without it reads the object's own `ETag` alone.
void ObjectStorageQueuePostProcessor::removeCopiedSource(
    const StoredObject & object, [[maybe_unused]] const SourceGeneration & consumed) const
{
#if USE_AWS_S3
    /// The copy consumed exactly this version, so the delete takes exactly this version too and
    /// leaves any newer one (and its delete marker) alone.
    if (!consumed.version_id.empty())
    {
        if (auto * s3_storage = dynamic_cast<S3ObjectStorage *>(object_storage.get()); s3_storage != nullptr)
        {
            s3_storage->removeObjectVersionIfExists(object, consumed.version_id);
            return;
        }
    }
#endif
    /// Without a version to pin, the key is deleted as a whole, pinned to the generation the copy
    /// consumed: `removeObjectIfExists` sends it as `If-Match`, so a source rewritten since then is
    /// refused with `FILE_CHANGED_DURING_READ` rather than deleted.
    object_storage->removeObjectIfExists(object);
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

/// Two lookups of one object can quote an Azure `ETag` differently; on S3 the value compares as is.
static bool isSameGeneration([[maybe_unused]] ObjectStorageType type, const String & lhs, const String & rhs)
{
#if USE_AZURE_BLOB_STORAGE
    if (type == ObjectStorageType::Azure)
        return AzureBlobStorage::normalizeETag(lhs) == AzureBlobStorage::normalizeETag(rhs);
#endif
    return lhs == rhs;
}

/// The version of `source` that was read; only S3 records one, so anything else has none to look up.
static std::optional<ObjectMetadata>
tryGetIngestedVersionMetadata([[maybe_unused]] const IObjectStorage & object_storage, [[maybe_unused]] const StoredObject & source)
{
#if USE_AWS_S3
    if (const auto * s3_storage = dynamic_cast<const S3ObjectStorage *>(&object_storage))
        return s3_storage->tryGetObjectVersionMetadata(source.remote_path, source.version_id, /*with_tags=*/false);
#endif
    return {};
}

void ObjectStorageQueuePostProcessor::moveWithinBucket(
    const StoredObjects & objects,
    const String & move_prefix,
    bool preserve_path,
    StoredObjects & successful_objects) const
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
    ChangedGeneration changed_generation;

    std::vector<UInt8> succeeded(objects.size(), 0);

    SCOPE_EXIT_SAFE({
        for (size_t i = 0; i < objects.size(); ++i)
            if (succeeded[i])
                successful_objects.emplace_back(objects[i]);
    });

    try
    {
        for (size_t objects_index = 0; objects_index < objects.size(); ++objects_index)
        {
            const auto & object_from = objects[objects_index];
            auto destination = applyMovePrefixIfPresent(object_from, move_prefix, preserve_path);
            /// Two objects of one batch can map to the same key while nothing is there yet, which no
            /// destination precondition can see: the second one is a collision all the same.
            if (!destinations.insert(destination.remote_path).second)
            {
                reportMoveCollision(object_from, destination);
                continue;
            }
            /// The task outlives this iteration, so it takes its own copy of the source object.
            task_tracker.add(
                [&, objects_index, source_object = object_from, object_to = std::move(destination)]
                {
                    try
                    {
                        auto copy_object = [&]() -> CopyResult
                        {
                            /// On Azure the move must be pinned to the ingested generation (see
                            /// `moveAzureBlobs`); an untagged source would make `copyObject` select
                            /// whatever generation exists now with a `HEAD`, and the source never
                            /// hands over an untagged Azure object.
                            if (type == ObjectStorageType::Azure && source_object.etag.empty())
                                throw Exception(
                                    ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                                    "Cannot move Azure blob {}: the generation that was ingested is not known",
                                    source_object.remote_path);

                            LOG_TRACE(log, "Copying object {} to {}", source_object.remote_path, object_to.remote_path);
                            std::optional<ObjectAttributes> provenance;
                            SourceGeneration consumed{.version_id = {}, .etag = source_object.etag};
                            auto write_settings = move_write_settings;
                            /// A preserved path needs no destination guard, but the move still acts on the
                            /// generation that was ingested, so the source is looked up whatever the guard is.
                            auto source_metadata
                                = object_storage->tryGetObjectMetadata(source_object.remote_path, /*with_tags=*/false);
                            /// Only the generation the rows were read from may be moved. Rethrown once the
                            /// batch is done, so the file is not committed and the newer generation is ingested.
                            if (source_metadata && !isSameGeneration(type, source_metadata->etag, source_object.etag))
                                throw Exception(
                                    type == ObjectStorageType::Azure ? ErrorCodes::FILE_CHANGED_DURING_READ
                                                                     : ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                                    "Object {} was not moved: it changed after it was ingested "
                                    "(its `ETag` is {} instead of {})",
                                    source_object.remote_path, source_metadata->etag, source_object.etag);
                            /// On a versioned bucket the key may hold a same-byte re-upload or nothing at all,
                            /// so everything below takes the version that was read, not whatever the key holds.
                            if (!source_object.version_id.empty()
                                && (!source_metadata || source_metadata->version_id != source_object.version_id))
                            {
                                /// Only a key that holds another version has a newer generation left to ingest.
                                const bool key_holds_another_version = source_metadata.has_value();
                                source_metadata = tryGetIngestedVersionMetadata(*object_storage, source_object);
                                if (!source_metadata)
                                    throw Exception(
                                        key_holds_another_version ? ErrorCodes::S3_OBJECT_CHANGED_DURING_READ : ErrorCodes::S3_ERROR,
                                        "Object {} was not moved: its version {} that was ingested is gone",
                                        source_object.remote_path, source_object.version_id);
                            }
                            if (source_metadata)
                            {
                                consumed.version_id = source_metadata->version_id;
                                /// Only a guarded move re-uploads the object, so only it stamps provenance for a
                                /// later attempt to recognise its own copy by.
                                if (!preserve_path)
                                    provenance = makeMoveProvenance(
                                        source_metadata->attributes,
                                        keeper_identity,
                                        source_object.remote_path,
                                        source_metadata->etag,
                                        source_metadata->last_modified.epochTime(),
                                        source_metadata->version_id);
                                /// The backend looks the source up again, so pin it to the generation this
                                /// lookup describes: a rewrite in between fails the copy instead of copying
                                /// newer bytes.
                                write_settings.object_storage_copy_source_if_match = source_metadata->etag;
                                /// Two generations of one key can share an `ETag`, so only the version names
                                /// the one this lookup, the copy and the delete all describe.
                                write_settings.object_storage_copy_source_version_id = source_metadata->version_id;
                                /// Park between the source lookup and the copy. No-op unless
                                /// explicitly enabled.
                                FailPointInjection::pauseFailPoint(
                                    FailPoints::object_storage_queue_pause_after_move_source_lookup);
                            }

                            try
                            {
                                object_storage->copyObject(
                                    source_object, object_to, read_settings, write_settings, provenance);
                            }
                            catch (const Exception & e)
                            {
                                if (e.code() != ErrorCodes::FILE_ALREADY_EXISTS)
                                    throw;

                                auto destination_metadata
                                    = object_storage->tryGetObjectMetadata(object_to.remote_path, /*with_tags=*/false);
                                return CopyResult{
                                    .destination_is_ours = destination_metadata
                                        && destinationIsOwnCommittedCopy(provenance, destination_metadata->attributes),
                                    .consumed = consumed};
                            }
                            return CopyResult{.destination_is_ours = true, .consumed = consumed};
                        };
                        switch (copyAndRemoveObject(source_object, copy_object))
                        {
                            case MoveResult::DestinationCollision:
                                reportMoveCollision(source_object, object_to);
                                return;
                            case MoveResult::Moved:
                                break;
                        }
                        succeeded[objects_index] = 1;
                        ++moved_objects;
                    }
                    catch (...)
                    {
                        changed_generation.rememberIfCurrentExceptionIsOne();
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

        std::erase_if(successful_objects, [](const StoredObject& object) { return object.remote_path.empty(); });

        throw;
    }
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, moved_objects);
    changed_generation.rethrowIfAny();
}

bool ObjectStorageQueuePostProcessor::deleteVersionedS3Objects(
    const StoredObjects & objects, StoredObjects & successful_objects) const
{
#if USE_AWS_S3
    auto * s3_storage = dynamic_cast<S3ObjectStorage *>(object_storage.get());
    if (!s3_storage)
        return false;

    /// Delete the exact ingested version of each object directly without re-resolving the key.
    /// If any object has no version_id, fall back to non-versioned batch delete.
    for (const auto & object : objects)
    {
        if (object.version_id.empty())
            return false;
    }

    for (const auto & object : objects)
    {
        s3_storage->removeObjectVersionIfExists(object, object.version_id);
        successful_objects.push_back(object);
    }
    return true;
#else
    (void)objects;
    (void)successful_objects;
    return false;
#endif
}


void ObjectStorageQueuePostProcessor::moveS3Objects(const StoredObjects & objects, StoredObjects & successful_objects) const
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
            ChangedGeneration changed_generation;
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
                    auto copy_object = [&]() -> CopyResult
                    {
                        /// The source never hands over an untagged object for a move (it fails such a
                        /// file instead of reading it), and its read was pinned to this very generation
                        /// (`afterProcessingNeedsIngestedGeneration`), so an untagged object here would
                        /// be moved as whatever generation exists now.
                        if (object_from.etag.empty())
                            throw Exception(
                                ErrorCodes::S3_ERROR,
                                "Cannot move S3 object {}: the generation that was ingested is not known",
                                object_from.remote_path);

                        auto source_info = S3::getObjectInfo(
                            *src_client,
                            src_bucket,
                            object_from.remote_path,
                            /*version_id=*/object_from.version_id,
                            /*with_metadata=*/true,
                            /*with_tags=*/false);
                        /// Only the generation the rows were read from may be moved. Rethrown once the
                        /// batch is done, so the file is not committed and the newer generation is ingested.
                        if (source_info.etag != object_from.etag)
                            throw Exception(
                                ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                                "S3 object {} was not moved: it changed after it was ingested (its `ETag` is {} instead of {})",
                                object_from.remote_path, source_info.etag, object_from.etag);
                        /// Everything below must describe the generation this HEAD saw, which the check above ties
                        /// to the one that was read: the provenance a later attempt matches against, the tags, and
                        /// the copied bytes. The version names that generation whatever the destination guard is, so
                        /// an unguarded move (a preserved path, a prefixless one) pins its copy and its delete to it
                        /// too instead of taking whatever the key holds by then. Empty on unversioned buckets.
                        const String & source_version_id = source_info.version_id;
                        /// A guarded move re-uploads the object, so the tags are read explicitly rather than through
                        /// the `HeadObject` tag count, which restricted credentials do not get to see.
                        std::optional<ObjectAttributes> source_tags;
                        if (!move_if_none_match.empty() && settings.after_processing_move_preserve_tags)
                            source_tags = S3::getObjectTags(
                                *src_client, src_bucket, object_from.remote_path, source_version_id);
                        const auto provenance = move_if_none_match.empty() ? std::optional<ObjectAttributes>{}
                                                                           : makeMoveProvenance(
                                                                                 source_info.metadata,
                                                                                 keeper_identity,
                                                                                 object_from.remote_path,
                                                                                 source_info.etag,
                                                                                 source_info.last_modification_time,
                                                                                 source_version_id);
                        /// What the copy below consumes, and therefore the only generation the delete
                        /// that follows it may remove.
                        const SourceGeneration consumed{source_version_id, source_info.etag};

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
                                /*fallback_file_reader=*/[&]() -> std::unique_ptr<SeekableReadBuffer>
                                {
                                    /// `readObject` pins the read to `object_from.etag`, which is the
                                    /// generation the copy is pinned to; a selected version needs its own reader.
                                    if (source_version_id.empty())
                                        return s3_storage->readObject(object_from, read_settings_to_use);
                                    return std::make_unique<ReadBufferFromS3>(
                                        src_client, src_bucket, object_from.remote_path, source_version_id,
                                        s3_settings->request_settings, read_settings_to_use,
                                        /*use_external_buffer=*/false, /*offset=*/0, /*read_until_position=*/0,
                                        /*restricted_seek=*/false, /*file_size=*/std::nullopt,
                                        /*credentials_refresh_callback=*/[]() -> std::unique_ptr<const S3::Client> { return nullptr; },
                                        /*blob_storage_log=*/nullptr, /*expected_etag=*/object_from.etag);
                                },
                                /*object_metadata=*/provenance,
                                S3CopyFileSettings{
                                    .if_none_match = move_if_none_match,
                                    .source_headers = move_if_none_match.empty() ? std::optional<S3::ObjectHeaders>{}
                                                                                 : std::optional<S3::ObjectHeaders>{source_info.headers},
                                    .source_tags = std::move(source_tags),
                                    .source_if_match = object_from.etag,
                                    .source_version_id = source_version_id});
                        }
                        catch (const Exception & e)
                        {
                            if (e.code() != ErrorCodes::FILE_ALREADY_EXISTS)
                                throw;

                            const auto destination_info = S3::getObjectInfoIfExists(
                                *dst_client, dst_uri.bucket, object_to.remote_path, /*version_id=*/{}, /*with_metadata=*/true);
                            return CopyResult{
                                .destination_is_ours
                                = destinationIsOwnCommittedCopy(provenance, destination_info.metadata),
                                .consumed = consumed};
                        }
                        return CopyResult{.destination_is_ours = true, .consumed = consumed};
                    };
                    switch (copyAndRemoveObject(object_from, copy_object))
                    {
                        case MoveResult::DestinationCollision:
                            reportMoveCollision(object_from, object_to);
                            continue;
                        case MoveResult::Moved:
                            break;
                    }

                    successful_objects.emplace_back(object_from);
                    moved_objects += 1;
                }
                catch (...)
                {
                    changed_generation.rememberIfCurrentExceptionIsOne();
                    LOG_WARNING(
                        log,
                        "Failed to move S3 object {} with exception: {}",
                        object_from.remote_path,
                        getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
                    );
                }
            }
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, moved_objects);
            changed_generation.rethrowIfAny();
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Underlying storage is not S3");
        }
    }
    else if (!move_prefix.empty())
    {
        moveWithinBucket(objects, move_prefix, settings.after_processing_move_preserve_path, successful_objects);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No settings to move S3 objects");
    }
#else
    UNUSED(objects);
    UNUSED(successful_objects);
#endif
}

void ObjectStorageQueuePostProcessor::moveAzureBlobs(const StoredObjects & objects, StoredObjects & successful_objects) const
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
            ChangedGeneration changed_generation;
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
                    auto copy_object = [&]() -> CopyResult
                    {
                        /// The source never hands over an untagged blob for a move, and its read was
                        /// pinned to this very generation, so an untagged blob here would be moved as
                        /// whatever generation exists now.
                        const String & src_etag = object_from.etag;
                        if (src_etag.empty())
                            throw Exception(
                                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                                "Cannot move Azure blob {}: the generation that was ingested is not known",
                                object_from.remote_path);

                        auto properties = src_client->GetBlobClient(object_from.remote_path).GetProperties().Value;
                        const String current_etag = AzureBlobStorage::getETagOrEmpty(properties.ETag);
                        /// Only the generation the rows were read from may be moved. Rethrown once the
                        /// batch is done, so the file is not committed and the newer generation is ingested.
                        if (AzureBlobStorage::normalizeETag(current_etag) != AzureBlobStorage::normalizeETag(src_etag))
                            throw Exception(
                                ErrorCodes::FILE_CHANGED_DURING_READ,
                                "Azure blob {} was not moved: it changed after it was ingested (its `ETag` is {} instead of {})",
                                object_from.remote_path, current_etag, src_etag);
                        const auto blob_size = properties.BlobSize;
                        /// Blob deletes take no version, so the delete that follows the copy is pinned to
                        /// this `ETag` instead of removing whatever the name points at by then.
                        const SourceGeneration consumed{/*version_id=*/{}, src_etag};
                        const auto provenance = move_if_none_match.empty()
                            ? std::optional<ObjectAttributes>{}
                            : makeMoveProvenance(
                                  ObjectAttributes{properties.Metadata.begin(), properties.Metadata.end()},
                                  keeper_identity,
                                  object_from.remote_path,
                                  current_etag,
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
                                blob_size,
                                move_container,
                                /* dest_blob */ object_to.remote_path,
                                request_settings,
                                read_settings,
                                provenance,
                                scheduler,
                                /* blob_storage_log */ {},
                                /* dest_if_none_match */ move_if_none_match,
                                /* src_etag */ src_etag);
                        }
                        catch (const Azure::Core::RequestFailedException & e)
                        {
                            if (!move_if_none_match.empty() && isAzureDestinationAlreadyExistsError(e))
                            {
                                auto destination_properties = dst_client->GetBlobClient(object_to.remote_path).GetProperties().Value;
                                return CopyResult{
                                    .destination_is_ours = destinationIsOwnCommittedCopy(
                                        provenance,
                                        ObjectAttributes{
                                            destination_properties.Metadata.begin(), destination_properties.Metadata.end()}),
                                    .consumed = consumed};
                            }
                            throw;
                        }
                        return CopyResult{.destination_is_ours = true, .consumed = consumed};
                    };
                    switch (copyAndRemoveObject(object_from, copy_object))
                    {
                        case MoveResult::DestinationCollision:
                            reportMoveCollision(object_from, object_to);
                            continue;
                        case MoveResult::Moved:
                            break;
                    }

                    successful_objects.emplace_back(object_from);

                    moved_objects += 1;
                }
                catch (...)
                {
                    changed_generation.rememberIfCurrentExceptionIsOne();
                    LOG_WARNING(
                        log,
                        "Failed to move Azure object {} with exception: {}",
                        object_from.remote_path,
                        getExceptionMessage(std::current_exception(), /*with_stacktrace=*/ false)
                    );
                }
            }
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, moved_objects);
            changed_generation.rethrowIfAny();
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Underlying storage is not Azure");
        }
    }
    else if (!move_prefix.empty())
    {
        moveWithinBucket(objects, move_prefix, settings.after_processing_move_preserve_path, successful_objects);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No settings to move Azure blobs");
    }
#else
    UNUSED(objects);
    UNUSED(successful_objects);
#endif
}

}
