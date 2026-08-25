#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/ObjectStorageKey.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.h>

#include <Common/ProxyConfigurationResolverProvider.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/deleteFileFromS3.h>
#include <Interpreters/Context.h>
#include <Common/quoteString.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Core/Settings.h>
#include <Common/BlobStorageLogWriter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>

#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/StackTrace.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <aws/s3/model/Tag.h>
#include <aws/s3/model/Tagging.h>

namespace ProfileEvents
{
    extern const Event S3ListObjects;
    extern const Event S3ListObjectsMicroseconds;
    extern const Event DiskS3DeleteObjects;
    extern const Event DiskS3ListObjects;
}

namespace CurrentMetrics
{
    extern const Metric ObjectStorageS3Threads;
    extern const Metric ObjectStorageS3ThreadsActive;
    extern const Metric ObjectStorageS3ThreadsScheduled;
}

namespace DB::FailPoints
{
    extern const char object_storage_force_refresh_callback_success[];
}


namespace DB
{
namespace Setting
{
    extern const SettingsBool s3_validate_request_settings;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool allow_native_copy;
    extern const S3RequestSettingsBool check_objects_after_upload;
    extern const S3RequestSettingsUInt64 list_object_keys_size;
    extern const S3RequestSettingsUInt64 objects_chunk_size_to_delete;
    extern const S3RequestSettingsUInt64 max_single_part_upload_size;
    extern const S3RequestSettingsUInt64 min_upload_part_size;
    extern const S3RequestSettingsUInt64 max_unexpected_write_error_retries;
    extern const S3RequestSettingsUInt64 max_single_operation_copy_size;
}


namespace S3AuthSetting
{
    extern const S3AuthSettingsString http_client;
}


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int S3_ERROR;
}

namespace
{

template <typename Result, typename Error>
void throwIfError(const Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw S3Exception(
            fmt::format("{} (Code: {}, S3 exception: '{}')",
                        err.GetMessage(), static_cast<size_t>(err.GetErrorType()), err.GetExceptionName()),
            err.GetErrorType());
    }
}

template <typename Result, typename Error, typename... Args>
void throwIfError(const Aws::Utils::Outcome<Result, Error> & response, fmt::format_string<Args...> context_fmt, Args &&... args)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw S3Exception(
            fmt::format("{} (Code: {}, S3 exception: '{}'), {}",
                        err.GetMessage(), static_cast<size_t>(err.GetErrorType()), err.GetExceptionName(),
                        fmt::format(context_fmt, std::forward<Args>(args)...)),
            err.GetErrorType());
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
        const std::string & bucket_,
        const std::string & path_prefix,
        std::shared_ptr<const S3::Client> client_,
        size_t max_list_size,
        bool with_tags_,
        const std::optional<std::string> & start_after_)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageS3Threads,
            CurrentMetrics::ObjectStorageS3ThreadsActive,
            CurrentMetrics::ObjectStorageS3ThreadsScheduled,
            ThreadName::S3_LIST_POOL)
        , client(client_)
        , request(std::make_unique<S3::ListObjectsV2Request>())
        , with_tags(with_tags_)
        , start_after_set(start_after_.has_value() && !start_after_->empty())
    {
        request->SetBucket(bucket_);
        request->SetPrefix(path_prefix);
        request->SetMaxKeys(static_cast<int>(max_list_size));
        if (start_after_set)
            request->SetStartAfter(*start_after_);
    }

    ~S3IteratorAsync() override
    {
        /// Deactivate background threads before resetting the request to avoid data race.
        deactivate();
        request.reset();
        client.reset();
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);

        Aws::S3::Model::ListObjectsV2Outcome outcome;

        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::S3ListObjectsMicroseconds);
            outcome = client->ListObjectsV2(*request);
        }

        /// Outcome failure will be handled on the caller side.
        if (outcome.IsSuccess())
        {
            const auto next_continuation_token = outcome.GetResult().GetNextContinuationToken();
            if (start_after_set)
            {
                /// StartAfter should only be sent on the first request. AWS SDK doesn't provide
                /// a way to clear "has been set" flag, so we rebuild request for pagination.
                auto paginated_request = std::make_unique<S3::ListObjectsV2Request>();
                paginated_request->SetBucket(request->GetBucket());
                paginated_request->SetPrefix(request->GetPrefix());
                paginated_request->SetMaxKeys(request->GetMaxKeys());
                paginated_request->SetContinuationToken(next_continuation_token);
                request = std::move(paginated_request);
                start_after_set = false;
            }
            else
            {
                request->SetContinuationToken(next_continuation_token);
            }

            auto objects = outcome.GetResult().GetContents();
            for (const auto & object : objects)
            {
                ObjectMetadata metadata{
                    .size_bytes = static_cast<uint64_t>(object.GetSize()),
                    .last_modified = Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()),
                    .etag = object.GetETag(),
                    .tags = {},
                    .attributes = {},
                };
                if (with_tags)
                    metadata.tags = S3::getObjectTags(*client, request->GetBucket(), object.GetKey());
                batch.emplace_back(std::make_shared<RelativePathWithMetadata>(object.GetKey(), std::move(metadata)));
            }

            /// It returns false when all objects were returned
            return outcome.GetResult().GetIsTruncated();
        }

        throw S3Exception(outcome.GetError().GetErrorType(),
                          "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                          quoteString(request->GetBucket()), quoteString(request->GetPrefix()),
                          backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
    }

    std::shared_ptr<const S3::Client> client;
    std::unique_ptr<S3::ListObjectsV2Request> request;
    const bool with_tags;
    bool start_after_set;
};

}

bool S3ObjectStorage::exists(const StoredObject & object) const
{
    auto settings_ptr = s3_settings.get();
    const bool e = S3::objectExists(*client.get(), uri.bucket, object.remote_path, {});
    return e;
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    bool use_external_buffer,
    bool restrict_seek) const
{
    auto settings_ptr = s3_settings.get();

    /// A query can override request settings (from its SETTINGS clause or profile). Apply them to a
    /// local copy so they affect only this read and don't stick around for later queries, same as writeObject.
    S3::S3RequestSettings request_settings = settings_ptr->request_settings;
    if (auto query_context = CurrentThread::tryGetQueryContext();
        query_context && !query_context->isBackgroundContext())
    {
        const auto & settings = query_context->getSettingsRef();
        request_settings.updateFromSettings(settings, /* if_changed */ true, settings[Setting::s3_validate_request_settings]);
    }

    BlobStorageLogWriterPtr blob_storage_log;
    if (read_settings.remote_fs_settings.enable_blob_storage_log)
    {
        blob_storage_log = BlobStorageLogWriter::create(disk_name);
        if (blob_storage_log)
            blob_storage_log->local_path = object.local_path;
    }

    return std::make_unique<ReadBufferFromS3>(
        client.get(),
        uri.bucket,
        object.remote_path,
        uri.version_id,
        request_settings,
        patchSettings(read_settings),
        use_external_buffer,
        /* offset */0,
        /* read_until_position */0,
        restrict_seek,
        object.bytes_size ? std::optional<size_t>(object.bytes_size) : std::nullopt,
        credentials_refresh_callback,
        std::move(blob_storage_log));
}

SmallObjectDataWithMetadata S3ObjectStorage::readSmallObjectAndGetObjectMetadata( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    size_t max_size_bytes,
    std::optional<size_t> read_hint) const
{
    auto buffer = readObject(object, read_settings, read_hint);
    SmallObjectDataWithMetadata result;
    WriteBufferFromString out(result.data);
    copyDataMaxBytes(*buffer, out, max_size_bytes);
    out.finalize();

    result.metadata = dynamic_cast<ReadBufferFromS3 *>(buffer.get())->getObjectMetadataFromTheLastRequest();
    return result;
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

    S3::S3RequestSettings request_settings = s3_settings.get()->request_settings;
    /// NOTE: For background operations settings are not propagated from session or query. They are taken from
    /// default user's .xml config. It's obscure and unclear behavior. For them it's always better
    /// to rely on settings from disk.
    if (auto query_context = CurrentThread::tryGetQueryContext();
        query_context && !query_context->isBackgroundContext())
    {
        const auto & settings = query_context->getSettingsRef();
        request_settings.updateFromSettings(settings, /* if_changed */ true, settings[Setting::s3_validate_request_settings]);
    }

    if (write_settings.s3_check_objects_after_upload_override)
        request_settings[S3RequestSetting::check_objects_after_upload] = *write_settings.s3_check_objects_after_upload_override;

    if (write_settings.s3_single_part_upload_max_bytes_override)
    {
        /// Keep the whole body in ONE buffered part so the single-PUT path stays available up to
        /// the cap (conditional writes on generation-token stores; see WriteSettings).
        request_settings[S3RequestSetting::max_single_part_upload_size]
            = write_settings.s3_single_part_upload_max_bytes_override;
        request_settings[S3RequestSetting::min_upload_part_size]
            = write_settings.s3_single_part_upload_max_bytes_override;
    }

    if (write_settings.s3_max_unexpected_write_error_retries_override)
    {
        /// WriteBufferFromS3's OWN retry loop (makeSinglepartUpload/completeMultipartUpload) reissues
        /// the identical request — WITH its If-None-Match/If-Match condition — on a NO_SUCH_KEY
        /// response; this sits ABOVE the S3 client, so a client-level profile override does not bound
        /// it. See WriteSettings.
        request_settings[S3RequestSetting::max_unexpected_write_error_retries]
            = write_settings.s3_max_unexpected_write_error_retries_override;
    }

    ThreadPoolCallbackRunnerUnsafe<void> scheduler;
    if (write_settings.s3_allow_parallel_part_upload)
        scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), ThreadName::REMOTE_FS_WRITE_THREAD_POOL);

    auto blob_storage_log = BlobStorageLogWriter::create(disk_name);
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    /// The SingleAttempt profile (e.g. CAS conditional writes, RFC cas-s3-timeout-retry-control) rides
    /// on WriteSettings instead of changing this disk's shared client — every other write keeps using
    /// client.get() and its normal retry policy unchanged. getSingleAttemptClient() is only invoked
    /// when actually selected, so a plain write never pays for building/locking the clone.
    std::shared_ptr<const S3::Client> used_client;
    if (write_settings.object_storage_retry_profile == ObjectStorageRetryProfile::SingleAttempt)
        used_client = getSingleAttemptClient();
    else
        used_client = client.get();

    return std::make_unique<WriteBufferFromS3>(
        used_client,
        uri.bucket,
        object.remote_path,
        write_settings.use_adaptive_write_buffer ? write_settings.adaptive_write_buffer_initial_size : buf_size,
        request_settings,
        std::move(blob_storage_log),
        attributes,
        std::move(scheduler),
        disk_write_settings);
}


ObjectStorageIteratorPtr S3ObjectStorage::iterate(
    const std::string & path_prefix,
    size_t max_keys,
    bool with_tags,
    const std::optional<std::string> & start_after) const
{
    auto settings_ptr = s3_settings.get();
    if (!max_keys)
        max_keys = settings_ptr->request_settings[S3RequestSetting::list_object_keys_size];
    return std::make_shared<S3IteratorAsync>(uri.bucket, path_prefix, client.get(), max_keys, with_tags, start_after);
}

void S3ObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    auto settings_ptr = s3_settings.get();

    S3::ListObjectsV2Request request;
    request.SetBucket(uri.bucket);
    request.SetPrefix(path);
    if (max_keys)
        request.SetMaxKeys(static_cast<int>(max_keys));
    else
        request.SetMaxKeys(static_cast<int>(settings_ptr->request_settings[S3RequestSetting::list_object_keys_size]));

    Aws::S3::Model::ListObjectsV2Outcome outcome;
    do
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);

        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::S3ListObjectsMicroseconds);
            outcome = client.get()->ListObjectsV2(request);
        }

        throwIfError(outcome, "while listing objects in bucket '{}' with prefix '{}' on disk '{}'", uri.bucket, path, disk_name);

        auto result = outcome.GetResult();
        auto objects = result.GetContents();

        if (objects.empty())
            break;

        for (const auto & object : objects)
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(
                object.GetKey(),
                ObjectMetadata{
                    .size_bytes = static_cast<uint64_t>(object.GetSize()),
                    .last_modified = Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()),
                    .etag = object.GetETag(),
                    .tags = {},
                    .attributes = {},
                }));

        if (max_keys)
        {
            ssize_t keys_left = static_cast<ssize_t>(max_keys) - children.size();
            if (keys_left <= 0)
                break;
            request.SetMaxKeys(static_cast<int>(keys_left));
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void S3ObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    auto blob_storage_log = BlobStorageLogWriter::create(disk_name);

    deleteFileFromS3(client.get(), uri.bucket, object.remote_path, if_exists,
                      blob_storage_log, object.local_path, object.bytes_size,
                      ProfileEvents::DiskS3DeleteObjects);
}

void S3ObjectStorage::removeObjectsImpl(const StoredObjects & objects, bool if_exists)
{
    if (objects.empty())
        return;

    Strings keys = collectRemotePaths(objects);

    auto blob_storage_log = BlobStorageLogWriter::create(disk_name);
    Strings local_paths_for_blob_storage_log;
    VectorWithMemoryTracking<size_t> file_sizes_for_blob_storage_log;
    if (blob_storage_log)
    {
        local_paths_for_blob_storage_log.reserve(objects.size());
        file_sizes_for_blob_storage_log.reserve(objects.size());
        for (const auto & object : objects)
        {
            local_paths_for_blob_storage_log.push_back(object.local_path);
            file_sizes_for_blob_storage_log.push_back(object.bytes_size);
        }
    }

    auto settings_ptr = s3_settings.get();

    deleteFilesFromS3(client.get(), uri.bucket, keys, if_exists,
                      s3_capabilities, settings_ptr->request_settings[S3RequestSetting::objects_chunk_size_to_delete],
                      blob_storage_log, local_paths_for_blob_storage_log, file_sizes_for_blob_storage_log,
                      ProfileEvents::DiskS3DeleteObjects);
}

void S3ObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectImpl(object, true);
}

void S3ObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    removeObjectsImpl(objects, true);
}

ConditionalRemoveResult S3ObjectStorage::removeObjectIfTokenMatches(const StoredObject & object, const std::string & etag)
{
    S3::DeleteObjectRequest request;
    request.SetBucket(uri.bucket);
    request.SetKey(object.remote_path);
    request.SetIfMatch(etag);
    /// This is a content-addressed exact-token DELETE: mark it eligible for the typed NativeConditional
    /// mode, so a GCS-native client can send the generation token this etag actually encodes.
    request.setNativeConditional();

    ProfileEvents::increment(ProfileEvents::DiskS3DeleteObjects);

    auto outcome = client.get()->DeleteObject(request);

    /// Mirror removeObjectImpl (deleteFileFromS3): every conditional delete lands in
    /// system.blob_storage_log too — GC reclaim was invisible there otherwise. TokenMismatch
    /// and NotFound are routine protocol outcomes, recorded with the S3 error for filtering.
    if (auto blob_storage_log = BlobStorageLogWriter::create(disk_name))
        blob_storage_log->addEvent(BlobStorageLogElement::EventType::Delete,
                                   uri.bucket, object.remote_path,
                                   object.local_path, object.bytes_size,
                                   /* elapsed_microseconds */ 0,
                                   outcome.IsSuccess() ? 0 : static_cast<Int32>(outcome.GetError().GetErrorType()),
                                   outcome.IsSuccess() ? "" : outcome.GetError().GetMessage());

    if (outcome.IsSuccess())
        return {ConditionalRemoveOutcome::Removed, outcome.GetResult().GetDeleteMarker()};

    const auto & err = outcome.GetError();

    /// The token did not match the current incarnation: the conditional delete is rejected with a 412
    /// (see `S3::isPreconditionFailedError` for the one policy). Callers treat 'mismatch' and 'gone'
    /// alike (re-validate); a genuine absence is disambiguated downstream by a HEAD re-check.
    if (S3::isPreconditionFailedError(err))
        return {ConditionalRemoveOutcome::TokenMismatch, false};

    /// The object no longer exists (404). Protocol callers treat 'mismatch' and 'gone' alike (re-validate).
    if (S3::isNotFoundError(err.GetErrorType()))
        return {ConditionalRemoveOutcome::NotFound, false};

    throw S3Exception(err.GetErrorType(),
        "{} (Code: {}, S3 exception: '{}') while conditionally removing object with path {} from S3",
        err.GetMessage(), static_cast<size_t>(err.GetErrorType()), err.GetExceptionName(), object.remote_path);
}

bool S3ObjectStorage::conditionalOpsUseGenerationTokens() const
{
    return client.get()->supportsGcsNativeConditionalRequests();
}

bool S3ObjectStorage::supportsCopyMode(ObjectStorageCopyMode mode) const
{
    return mode == ObjectStorageCopyMode::Default
        || (mode == ObjectStorageCopyMode::NativeOnly
            && s3_settings.get()->request_settings[S3RequestSetting::allow_native_copy]);
}

void S3ObjectStorage::pinConditionalOpsGenerationDialect(bool expect_generation_tokens)
{
    pinned_generation_dialect.store(expect_generation_tokens ? 1 : 0);
}

std::optional<bool> S3ObjectStorage::isBucketVersioningEnabled() const
{
    S3::GetBucketVersioningRequest request;
    request.SetBucket(uri.bucket);

    auto outcome = client.get()->GetBucketVersioning(request);
    if (!outcome.IsSuccess())
        return std::nullopt;

    return outcome.GetResult().GetStatus() == Aws::S3::Model::BucketVersioningStatus::Enabled;
}

static void putObjectsTagOnS3(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & bucket,
    const Strings & object_keys,
    const String & tag_key,
    const String & tag_value
)
{
    auto log = getLogger("putObjectsTagOnS3");

    for (const String & object_key : object_keys)
    {
        S3::GetObjectTaggingRequest get_request;
        get_request.SetBucket(bucket);
        get_request.SetKey(object_key);

        auto get_outcome = s3_client->GetObjectTagging(get_request);
        if (!get_outcome.IsSuccess())
        {
            const auto & err = get_outcome.GetError();
            throw S3Exception(err.GetErrorType(), "{} (Code: {}) while getting tagging of S3 object path {}",
                              err.GetMessage(), static_cast<size_t>(err.GetErrorType()), object_key);
        }
        const auto & get_result = get_outcome.GetResult();
        const Aws::Vector<Aws::S3::Model::Tag> & existing_tag_set = get_result.GetTagSet();
        const bool present = (
            std::find_if(
                existing_tag_set.begin(),
                existing_tag_set.end(),
                [&] (const Aws::S3::Model::Tag& tag)
                {
                    return tag.GetKey() == tag_key && tag.GetValue() == tag_value;
                })
            != existing_tag_set.end());
        if (present)
        {
            LOG_TRACE(log, "S3 object path {} skipped as it already had the tag {}={}", object_key, tag_key, tag_value);
            continue;
        }
        Aws::Vector<Aws::S3::Model::Tag> tag_set = existing_tag_set;
        tag_set.push_back(Aws::S3::Model::Tag()
            .WithKey(tag_key)
            .WithValue(tag_value));

        S3::PutObjectTaggingRequest put_request;
        put_request.SetBucket(bucket);
        put_request.SetKey(object_key);
        put_request.SetTagging(Aws::S3::Model::Tagging().WithTagSet(tag_set));

        auto put_outcome = s3_client->PutObjectTagging(put_request);

        if (put_outcome.IsSuccess())
        {
            LOG_TRACE(log, "Tags of S3 object {} updated", object_key);
        }
        else
        {
            const auto & err = put_outcome.GetError();
            throw S3Exception(err.GetErrorType(), "{} (Code: {}) while putting tagging on S3 object {}",
                              err.GetMessage(), static_cast<size_t>(err.GetErrorType()), object_key);
        }
    }

}

void S3ObjectStorage::tagObjects(const StoredObjects & objects, const std::string & tag_key, const std::string & tag_value)
{
    Strings keys = collectRemotePaths(objects);
    putObjectsTagOnS3(client.get(), uri.bucket, keys, tag_key, tag_value);
}

std::optional<ObjectMetadata> S3ObjectStorage::tryGetObjectMetadata(const std::string & path, bool with_tags) const
{
    return tryGetObjectMetadataImpl(path, with_tags, ObjectStorageRequestMode::Default);
}

std::optional<ObjectMetadata> S3ObjectStorage::tryGetObjectMetadataWithNativeToken(const std::string & path, bool with_tags) const
{
    return tryGetObjectMetadataImpl(path, with_tags, ObjectStorageRequestMode::NativeConditional);
}

std::optional<ObjectMetadata> S3ObjectStorage::tryGetObjectMetadataImpl(const std::string & path, bool with_tags, ObjectStorageRequestMode request_mode) const
{
    auto settings_ptr = s3_settings.get();
    auto object_info = S3::getObjectInfoIfExists(
        *client.get(), uri.bucket, path, {}, /* with_metadata= */ true, with_tags, request_mode);

    if (object_info.size == 0 && object_info.last_modification_time == 0 && object_info.metadata.empty())
        return {};

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.is_size_known = object_info.is_size_known;
    result.last_modified = Poco::Timestamp::fromEpochTime(object_info.last_modification_time);
    result.etag = object_info.etag;
    result.tags = object_info.tags;
    result.attributes = object_info.metadata;

    return result;
}

ObjectMetadata S3ObjectStorage::getObjectMetadata(const std::string & path, bool with_tags) const
{
    auto settings_ptr = s3_settings.get();
    S3::ObjectInfo object_info;
    try
    {
        object_info = S3::getObjectInfo(*client.get(), uri.bucket, path, /*version_id=*/ {}, /*with_metadata=*/ true, /*with_tags=*/ with_tags);
    }
    catch (DB::Exception & e)
    {
        bool updated = false;
        if (credentials_refresh_callback)
        {
            auto new_client = credentials_refresh_callback();
            if (new_client)
            {
                client.set(std::move(new_client));
                object_info = S3::getObjectInfo(*client.get(), uri.bucket, path, /*version_id=*/ {}, /*with_metadata=*/ true, /*with_tags=*/ with_tags);
                updated = true;
            }
        }
        if (!updated)
        {
            e.addMessage("while reading '{}' in bucket '{}' on disk '{}'", path, uri.bucket, disk_name);
            throw;
        }
    }

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.is_size_known = object_info.is_size_known;
    result.last_modified = Poco::Timestamp::fromEpochTime(object_info.last_modification_time);
    result.etag = object_info.etag;
    result.tags = std::move(object_info.tags);
    result.attributes = object_info.metadata;

    return result;
}

void S3ObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    /// Shortcut for S3
    if (auto * dest_s3 = dynamic_cast<S3ObjectStorage * >(&object_storage_to); dest_s3 != nullptr)
    {
        auto current_client = dest_s3->client.get();
        auto settings_ptr = s3_settings.get();
        auto size = S3::getObjectSize(*client.get(), uri.bucket, object_from.remote_path, {});
        auto scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), ThreadName::S3_COPY_POOL);
        const auto read_settings_to_use = patchSettings(read_settings);

        try
        {
            copyS3File(
                /*src_s3_client=*/current_client,
                /*src_bucket=*/uri.bucket,
                /*src_key=*/object_from.remote_path,
                /*src_offset=*/0,
                /*src_size=*/size,
                /*dest_s3_client=*/current_client,
                /*dest_bucket=*/dest_s3->uri.bucket,
                /*dest_key=*/object_to.remote_path,
                settings_ptr->request_settings,
                read_settings_to_use,
                BlobStorageLogWriter::create(disk_name),
                scheduler,
                [&, this]{ return readObject(object_from, read_settings_to_use);},
                object_to_attributes,
                write_settings.object_storage_copy_mode);
            return;
        }
        catch (S3Exception & exc)
        {
            /// Default mode may fall through to a buffered copy after an authentication/permissions error;
            /// NativeOnly must preserve the native-copy failure.
            if (write_settings.object_storage_copy_mode == ObjectStorageCopyMode::NativeOnly
                || exc.getS3ErrorCode() != Aws::S3::S3Errors::ACCESS_DENIED)
                throw;
            else
            {
                bool updated = false;
                if (credentials_refresh_callback)
                {
                    auto new_client = credentials_refresh_callback();
                    if (new_client)
                    {
                        updated = true;
                        client.set(std::move(new_client));
                    }
                }
                if (!updated)
                    throw;
            }
            LOG_WARNING(getLogger("S3ObjectStorage"),
                "S3-server-side copy object from the disk {} to the disk {} can not be performed: {}\n",
                getName(), dest_s3->getName(), exc.what());
        }
    }

    if (write_settings.object_storage_copy_mode == ObjectStorageCopyMode::NativeOnly)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Native-only object copy requires both object storages to use the native S3 copy path");

    IObjectStorage::copyObjectToAnotherObjectStorage(object_from, object_to, read_settings, write_settings, object_storage_to, object_to_attributes);
}

void S3ObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (!supportsCopyMode(write_settings.object_storage_copy_mode))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Native-only object copy requires the native S3 copy path, which is disabled "
            "(allow_native_copy=false) for object storage {}",
            getName());

    auto current_client = client.get();
    auto settings_ptr = s3_settings.get();
    auto size = S3::getObjectSize(*current_client, uri.bucket, object_from.remote_path, {});
    auto scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), ThreadName::S3_COPY_POOL);
    const auto read_settings_to_use = patchSettings(read_settings);

    copyS3File(
        /*src_s3_client=*/current_client,
        /*src_bucket=*/uri.bucket,
        /*src_key=*/object_from.remote_path,
        /*src_offset=*/0,
        /*src_size=*/size,
        /*dest_s3_client=*/current_client,
        /*dest_bucket=*/uri.bucket,
        /*dest_key=*/object_to.remote_path,
        settings_ptr->request_settings,
        read_settings_to_use,
        BlobStorageLogWriter::create(disk_name),
        scheduler,
        [&, this]{ return readObject(object_from, read_settings_to_use);},
        object_to_attributes,
        write_settings.object_storage_copy_mode);
}

void S3ObjectStorage::shutdown()
{
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    const_cast<S3::Client &>(*client.get()).DisableRequestProcessing();
}

void S3ObjectStorage::startup()
{
    /// Need to be enabled if it was disabled during shutdown() call.
    const_cast<S3::Client &>(*client.get()).EnableRequestProcessing();
}

void S3ObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    const ApplyNewSettingsOptions & options)
{
    std::unique_ptr<S3Settings> settings_from_config = std::make_unique<S3Settings>();

    settings_from_config->loadFromConfigForObjectStorage(
        config, config_prefix, context->getSettingsRef(), uri.uri.getScheme(), context->getSettingsRef()[Setting::s3_validate_request_settings]);

    auto modified_settings = std::make_unique<S3Settings>(*s3_settings.get());

    auto apply_endpoint_settings = [&]
    {
        if (auto endpoint_settings = context->getStorageS3Settings().getSettings(uri.uri.toString(), context->getUserName()))
        {
            modified_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
            modified_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
        }
    };

    auto apply_config_settings = [&]
    {
        modified_settings->auth_settings.updateIfChanged(settings_from_config->auth_settings);
        modified_settings->request_settings.updateIfChanged(settings_from_config->request_settings);
    };

    /// When a setting is given both in the general config and for a specific endpoint, the more specific
    /// one should win. For a disk the config is the disk's own section (more specific than an endpoint
    /// block), so apply it last. For S3/S3Queue tables the config is the general <s3> section (less
    /// specific than an endpoint block), so apply the endpoint last instead. Whichever is applied last wins.
    if (for_disk_s3)
    {
        apply_endpoint_settings();
        apply_config_settings();
    }
    else
    {
        apply_config_settings();
        apply_endpoint_settings();
    }

    modified_settings->request_settings.proxy_resolver = DB::ProxyConfigurationResolverProvider::getFromOldSettingsFormat(
        ProxyConfiguration::protocolFromString(uri.uri.getScheme()), config_prefix, config);

    /// A caller that derived persistent state from the conditional-ops dialect pinned it (see
    /// `IObjectStorage::pinConditionalOpsGenerationDialect`). Refuse before the client is replaced, so a
    /// rejected reload leaves the working client and its dialect in place.
    ///
    /// This is the only point where the question can be answered: `modified_settings` above is the merge
    /// of the current settings, any endpoint-level block and the disk's own section, and `http_client`
    /// may be set by any of them. Checking a single config section instead would miss an endpoint-level
    /// flip entirely, and would refuse a reload that changes nothing whenever the effective value comes
    /// from somewhere other than that section.
    if (const int8_t pinned = pinned_generation_dialect.load(); pinned >= 0)
    {
        const bool would_be_generation
            = S3::httpClientImpliesGcsGenerationDialect(modified_settings->auth_settings[S3AuthSetting::http_client]);
        if (would_be_generation != (pinned == 1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Object storage {} cannot change its conditional-operation dialect on reload: it is in "
                "use by a mount that has already recorded {} incarnation tokens, and the new settings "
                "resolve `http_client` to '{}', which would mint {} ones. Persisted tokens would no "
                "longer be comparable. Keep the previous `http_client`, or recreate the mount.",
                getName(),
                pinned == 1 ? "generation" : "ETag",
                modified_settings->auth_settings[S3AuthSetting::http_client].value,
                would_be_generation ? "generation" : "ETag");
    }

    auto current_settings = s3_settings.get();
    if (options.allow_client_change
        && (current_settings->auth_settings.hasUpdates(modified_settings->auth_settings) || for_disk_s3))
    {
        auto new_client = getClient(uri, *modified_settings, context, for_disk_s3, disk_name);
        client.set(std::move(new_client));
    }
    s3_settings.set(std::move(modified_settings));
}

ObjectStorageKeyGeneratorPtr S3ObjectStorage::createKeyGenerator() const
{
    if (!key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key generator is not set");

    return key_generator;
}

std::shared_ptr<const S3::Client> S3ObjectStorage::getS3StorageClient()
{
    return client.get();
}

std::shared_ptr<const S3::Client> S3ObjectStorage::tryGetS3StorageClient()
{
    return client.get();
}

std::shared_ptr<const S3::Client> S3ObjectStorage::getSingleAttemptClient() const
{
    auto base = client.get();
    std::lock_guard lock(single_attempt_client_mutex);
    if (single_attempt_client && single_attempt_client_base == base)
        return single_attempt_client;

    auto cfg = base->getClientConfiguration();
    cfg.retry_strategy.max_retries = 0;
    cfg.retryStrategy = std::make_shared<S3::SingleAttemptRetryStrategy>();

    /// A server can reject an If-Match/If-None-Match request before accepting its body; waiting for
    /// the 100-continue response avoids uploading a large body that cannot commit. Respect the
    /// disk's configured expect_continue_min_bytes; if unset, use the established 1 MiB floor.
    static constexpr uint64_t fallback_expect_continue_min_bytes = 1024 * 1024;
    if (cfg.expect_continue_min_bytes == 0)
        cfg.expect_continue_min_bytes = fallback_expect_continue_min_bytes;

    single_attempt_client = base->cloneWithConfigurationOverride(cfg);
    single_attempt_client_base = base;
    return single_attempt_client;
}

bool S3ObjectStorage::tryRefreshCredentialsViaCallback()
{
    fiu_do_on(FailPoints::object_storage_force_refresh_callback_success, { return true; });

    if (!credentials_refresh_callback)
        return false;
    auto new_client = credentials_refresh_callback();
    if (!new_client)
        return false;
    client.set(std::move(new_client));
    return true;
}
}

#endif
