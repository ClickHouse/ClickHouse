#include <ranges>
#include <algorithm>
#include <exception>
#include <optional>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/Stopwatch.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/getRandomASCIIString.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>

#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>

#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <Interpreters/Context.h>


namespace CurrentMetrics
{
    extern const Metric ObjectStorageAzureThreads;
    extern const Metric ObjectStorageAzureThreadsActive;
    extern const Metric ObjectStorageAzureThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event AzureListObjects;
    extern const Event DiskAzureListObjects;
    extern const Event AzureDeleteObjects;
    extern const Event DiskAzureDeleteObjects;
    extern const Event AzureGetProperties;
    extern const Event DiskAzureGetProperties;
    extern const Event AzureCopyObject;
    extern const Event DiskAzureCopyObject;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int UNSUPPORTED_METHOD;
    extern const int FILE_CHANGED_DURING_READ;
}

namespace
{

class AzureIteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    AzureIteratorAsync(
        const std::string & path_prefix,
        std::shared_ptr<const AzureBlobStorage::ContainerClient> client_,
        size_t max_list_size)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageAzureThreads,
            CurrentMetrics::ObjectStorageAzureThreadsActive,
            CurrentMetrics::ObjectStorageAzureThreadsScheduled,
            ThreadName::AZURE_LIST_POOL)
        , client(client_)
    {
        options.Prefix = path_prefix;
        options.PageSizeHint = static_cast<int>(max_list_size);
    }

    ~AzureIteratorAsync() override
    {
        if (!deactivated)
            deactivate();
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::AzureListObjects);
        if (client->IsClientForDisk())
            ProfileEvents::increment(ProfileEvents::DiskAzureListObjects);

        chassert(batch.empty());
        auto blob_list_response = client->ListBlobs(options);
        auto blobs_list = blob_list_response.Blobs;
        batch.reserve(blobs_list.size());

        for (const auto & blob : blobs_list)
        {
            batch.emplace_back(std::make_shared<RelativePathWithMetadata>(
                blob.Name,
                ObjectMetadata{
                    .size_bytes = static_cast<uint64_t>(blob.BlobSize),
                    .last_modified = Poco::Timestamp::fromEpochTime(
                        std::chrono::duration_cast<std::chrono::seconds>(
                            static_cast<std::chrono::system_clock::time_point>(blob.Details.LastModified).time_since_epoch()).count()),
                    .etag = AzureBlobStorage::getETagOrEmpty(blob.Details.ETag),
                    .tags = {},
                    .attributes = {},
                }));
        }

        if (!blob_list_response.NextPageToken.HasValue() || blob_list_response.NextPageToken.Value().empty())
            return false;

        options.ContinuationToken = blob_list_response.NextPageToken;
        return true;
    }

    std::shared_ptr<const AzureBlobStorage::ContainerClient> client;
    Azure::Storage::Blobs::ListBlobsOptions options;
};

}


AzureObjectStorage::AzureObjectStorage(
    const String & name_,
    AzureBlobStorage::AuthMethod auth_method_,
    ClientPtr && client_,
    SettingsPtr && settings_,
    const AzureBlobStorage::ConnectionParams & connection_params_,
    const String & object_namespace_,
    const String & description_,
    const String & common_key_prefix_)
    : name(name_)
    , auth_method(std::move(auth_method_))
    , client(std::move(client_))
    , settings(std::move(settings_))
    , object_namespace(object_namespace_)
    , description(description_)
    , common_key_prefix(common_key_prefix_)
    , connection_params(connection_params_)
    , log(getLogger("AzureObjectStorage"))
{
}

ObjectStorageKeyGeneratorPtr AzureObjectStorage::createKeyGenerator() const
{
    return createObjectStorageKeyGeneratorByTemplate("[a-z]{32}");
}

bool AzureObjectStorage::exists(const StoredObject & object) const
{
    auto client_ptr = client.get();

    ProfileEvents::increment(ProfileEvents::AzureGetProperties);
    if (client_ptr->IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureGetProperties);

    try
    {
        auto blob_client = client_ptr->GetBlobClient(object.remote_path);
        blob_client.GetProperties();
        return true;
    }
    catch (const Azure::Storage::StorageException & e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            return false;
        throw;
    }
}

ObjectStorageIteratorPtr AzureObjectStorage::iterate(
    const std::string & path_prefix,
    size_t max_keys,
    bool,
    const std::optional<std::string> &) const
{
    /// start_after is ignored; the resume-from-key optimization is only used for S3 for now.
    auto settings_ptr = settings.get();
    auto client_ptr = client.get();

    return std::make_shared<AzureIteratorAsync>(path_prefix, client_ptr, max_keys ? max_keys : settings_ptr->list_object_keys_size);
}

void AzureObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    auto client_ptr = client.get();

    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = path;
    if (max_keys)
        options.PageSizeHint = max_keys;
    else
        options.PageSizeHint = settings.get()->list_object_keys_size;

    /// Re-issue ListBlobs per page through the client wrapper (which strips the endpoint prefix); the SDK's
    /// MoveToNextPage refetches pages 2..N directly and would leave the raw Azure prefix on their blob names.
    while (true)
    {
        auto blob_list_response = client_ptr->ListBlobs(options);

        ProfileEvents::increment(ProfileEvents::AzureListObjects);
        if (client_ptr->IsClientForDisk())
            ProfileEvents::increment(ProfileEvents::DiskAzureListObjects);

        for (const auto & blob : blob_list_response.Blobs)
        {
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(
                blob.Name,
                ObjectMetadata{
                    .size_bytes = static_cast<uint64_t>(blob.BlobSize),
                    .last_modified = Poco::Timestamp::fromEpochTime(
                        std::chrono::duration_cast<std::chrono::seconds>(
                            static_cast<std::chrono::system_clock::time_point>(blob.Details.LastModified).time_since_epoch()).count()),
                    .etag = AzureBlobStorage::getETagOrEmpty(blob.Details.ETag),
                    .tags = {},
                    .attributes = {},
                }));
        }

        if (max_keys && children.size() >= max_keys)
            break;

        if (!blob_list_response.NextPageToken.HasValue() || blob_list_response.NextPageToken.Value().empty())
            break;

        options.ContinuationToken = blob_list_response.NextPageToken;
    }
}

std::unique_ptr<ReadBufferFromFileBase> AzureObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    bool use_external_buffer,
    bool restrict_seek) const
{
    auto settings_ptr = settings.get();

    BlobStorageLogWriterPtr blob_storage_log;
    if (read_settings.remote_fs_settings.enable_blob_storage_log)
    {
        blob_storage_log = BlobStorageLogWriter::create(name);
        if (blob_storage_log)
            blob_storage_log->local_path = object.local_path;
    }

    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client.get(),
        object.remote_path,
        patchSettings(read_settings),
        settings_ptr->max_single_read_retries,
        settings_ptr->max_single_download_retries,
        use_external_buffer,
        restrict_seek,
        /* read_until_position */0,
        std::move(blob_storage_log),
        connection_params.getContainer(),
        /// `bytes_size` may be the `StoredObject::UnknownSize` sentinel for an object whose size
        /// was never determined; it is not a real size, so it must not become an end-of-file bound.
        /// Any other value, zero included, is a real size: it was obtained from the `LIST` or `HEAD`
        /// that produced the `StoredObject` before the read started (every Azure carrier reports the
        /// `BlobSize` of the listing or of the properties). It is the length of the file for every
        /// layer above the buffer, so the read ends there - see
        /// `ReadBufferFromAzureBlobStorage::known_object_size`.
        object.bytes_size != StoredObject::UnknownSize ? std::optional<size_t>(object.bytes_size) : std::nullopt,
        /// Pin every request of this read to the generation of the blob seen at read setup, so an
        /// in-place overwrite cannot splice two generations into one logical read.
        object.etag);
}

SmallObjectDataWithMetadata AzureObjectStorage::readSmallObjectAndGetObjectMetadata( /// NOLINT
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

    result.metadata = dynamic_cast<ReadBufferFromAzureBlobStorage *>(buffer.get())->getObjectMetadataFromTheLastRequest();
    return result;
}


std::unique_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient>
AzureObjectStorage::buildDataLakeFileClient(const String & blob_path) const
{
    return std::make_unique<Azure::Storage::Files::DataLake::DataLakeFileClient>(
        makeAdlsGen2FileClient(
            connection_params.endpoint,
            auth_method,
            connection_params.client_options,
            blob_path));
}

std::unique_ptr<WriteBufferFromFileBase> AzureObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes>,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Azure storage doesn't support append");

    LOG_TEST(log, "Writing file: {}", object.remote_path);

    auto blob_storage_log = BlobStorageLogWriter::create(name);
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    ThreadPoolCallbackRunnerUnsafe<void> scheduler;
    if (write_settings.azure_allow_parallel_part_upload)
        scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), ThreadName::REMOTE_FS_WRITE_THREAD_POOL);

    if (isAdlsGen2Endpoint(connection_params.endpoint))
    {
        return std::make_unique<WriteBufferFromAzureDataLakeStorage>(
            connection_params.endpoint,
            auth_method,
            connection_params.client_options,
            object.remote_path,
            /// The adaptive initial size must not exceed buf_size (the maximum); this writer
            /// forwards the value straight to the allocator, so an out-of-range adaptive
            /// initial size would otherwise abort the server (see WriteBufferFromFileDescriptor).
            write_settings.use_adaptive_write_buffer ? std::min(write_settings.adaptive_write_buffer_initial_size, buf_size) : buf_size,
            patchSettings(write_settings),
            settings.get(),
            connection_params.getContainer(),
            std::move(blob_storage_log));
    }

    return std::make_unique<WriteBufferFromAzureBlobStorage>(
        client.get(),
        object.remote_path,
        write_settings.use_adaptive_write_buffer ? write_settings.adaptive_write_buffer_initial_size : buf_size,
        patchSettings(write_settings),
        settings.get(),
        connection_params.getContainer(),
        std::move(blob_storage_log),
        std::move(scheduler));
}

void AzureObjectStorage::removeObjectImpl(
    const StoredObject & object,
    const std::shared_ptr<const AzureBlobStorage::ContainerClient> & client_ptr,
    bool if_exists,
    BlobStorageLogWriterPtr blob_storage_log,
    StoredObjects * successful_objects)
{
    ProfileEvents::increment(ProfileEvents::AzureDeleteObjects);
    if (client_ptr->IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureDeleteObjects);

    const auto & path = object.remote_path;
    LOG_TEST(log, "Removing single object: {}", path);

    /// A `StoredObject` that carries an `ETag` names one generation of the blob, not just a path:
    /// the delete is then pinned to that generation with `If-Match`, so a blob that was overwritten
    /// after the caller looked at it (a `MOVE` copies the generation it selected, then deletes)
    /// is left in place instead of being deleted without the newer generation having been seen.
    /// The header wants the quoted entity-tag form, while a tag from a listing is bare.
    const bool pinned_to_etag = !object.etag.empty();
    const Azure::ETag if_match = pinned_to_etag ? Azure::ETag(AzureBlobStorage::toQuotedETag(object.etag)) : Azure::ETag();

    Stopwatch watch;
    Int32 error_code = 0;
    String error_message;
    bool success = false;
    try
    {
        if (isAdlsGen2Endpoint(connection_params.endpoint))
        {
            Azure::Storage::Files::DataLake::DeleteFileOptions options;
            options.AccessConditions.IfMatch = if_match;
            buildDataLakeFileClient(path)->Delete(options);
            success = true;
        }
        else
        {
            Azure::Storage::Blobs::DeleteBlobOptions options;
            options.AccessConditions.IfMatch = if_match;
            auto delete_info = client_ptr->GetBlobClient(path).Delete(options);
            success = delete_info.Value.Deleted;
            if (!if_exists && !delete_info.Value.Deleted)
                throw Exception(
                    ErrorCodes::AZURE_BLOB_STORAGE_ERROR, "Failed to delete file (path: {}) in AzureBlob Storage, reason: {}",
                    path, delete_info.RawResponse ? delete_info.RawResponse->GetReasonPhrase() : "Unknown");
        }
    }
    catch (const Azure::Storage::StorageException & e)
    {
        error_code = static_cast<Int32>(e.StatusCode);
        error_message = e.Message;

        /// The precondition did not hold: the blob is not the generation the caller selected, so
        /// nothing was deleted. This is not "the object does not exist" and must not be swallowed
        /// by `if_exists`; the caller decides whether to look at the new generation and start over.
        if (pinned_to_etag && e.StatusCode == Azure::Core::Http::HttpStatusCode::PreconditionFailed)
        {
            if (blob_storage_log)
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Delete,
                    /* bucket */ connection_params.getContainer(),
                    /* remote_path */ path,
                    object.local_path,
                    object.bytes_size,
                    watch.elapsedMicroseconds(),
                    error_code,
                    error_message);
            throw Exception(
                ErrorCodes::FILE_CHANGED_DURING_READ,
                "Object {} was not deleted: it changed after it was selected (its `ETag` is no longer {})",
                path, object.etag);
        }

        if (!if_exists)
        {
            if (blob_storage_log)
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Delete,
                    /* bucket */ connection_params.getContainer(),
                    /* remote_path */ path,
                    object.local_path,
                    object.bytes_size,
                    watch.elapsedMicroseconds(),
                    error_code,
                    error_message);
            throw;
        }

        /// If object doesn't exist.
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
        {
            auto elapsed = watch.elapsedMicroseconds();
            if (blob_storage_log)
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Delete,
                    /* bucket */ connection_params.getContainer(),
                    /* remote_path */ path,
                    object.local_path,
                    object.bytes_size,
                    elapsed,
                    error_code,
                    error_message);

            if (successful_objects)
                successful_objects->emplace_back(object);

            return;
        }

        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    if (successful_objects)
        successful_objects->emplace_back(object);

    auto elapsed = watch.elapsedMicroseconds();

    if (blob_storage_log)
        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Delete,
            /* bucket */ connection_params.getContainer(),
            /* remote_path */ path,
            object.local_path,
            object.bytes_size,
            elapsed,
            success ? 0 : error_code,
            success ? "" : error_message);
}

void AzureObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto blob_storage_log = BlobStorageLogWriter::create(name);
    removeObjectImpl(object, client.get(), true, std::move(blob_storage_log));
}

void AzureObjectStorage::removeObjectsBatchIfExists(
    const StoredObjects & objects,
    const std::shared_ptr<const AzureBlobStorage::ContainerClient> & client_ptr,
    BlobStorageLogWriterPtr blob_storage_log,
    StoredObjects * successful_objects)
{
    /// https://github.com/Azure/azure-sdk-for-python/issues/22821#issuecomment-1024753986
    static constexpr size_t AZURE_BATCH_MAX_SUBREQUESTS = 256;

    const String & container = connection_params.getContainer();
    const bool is_disk = client_ptr->IsClientForDisk();
    const auto add_log_entry = [&](const StoredObject & object, size_t elapsed_mu, int32_t error_code = 0, const std::string & error_message = "")
    {
        if (!blob_storage_log)
            return;

        try
        {
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Delete,
                container, object.remote_path, object.local_path, object.bytes_size,
                elapsed_mu, error_code, error_message);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    };

    StoredObjectsSpan rest_objects = objects;
    while (!rest_objects.empty())
    {
        auto object_batch = rest_objects.first(std::min(rest_objects.size(), AZURE_BATCH_MAX_SUBREQUESTS));
        SCOPE_EXIT({ rest_objects = rest_objects.last(rest_objects.size() - object_batch.size()); });

        Stopwatch watch;
        AzureBlobStorage::BlobContainerBatch requests = client_ptr->CreateBatch();
        std::vector<AzureBlobStorage::DeleteBlobResultDeferredResponse> responses;
        for (const auto & object : object_batch)
        {
            /// As in `removeObjectImpl`: an object that carries an `ETag` is one generation of the
            /// blob, and only that generation is deleted.
            Azure::Storage::Blobs::DeleteBlobOptions options;
            if (!object.etag.empty())
                options.AccessConditions.IfMatch = Azure::ETag(AzureBlobStorage::toQuotedETag(object.etag));
            responses.push_back(requests.DeleteBlob(client_ptr->GetBlobPath(object.remote_path), options));
        }

        ProfileEvents::increment(ProfileEvents::AzureDeleteObjects, object_batch.size());
        if (is_disk)
            ProfileEvents::increment(ProfileEvents::DiskAzureDeleteObjects, object_batch.size());

        try
        {
            client_ptr->SubmitBatch(requests);
        }
        catch (const Azure::Storage::StorageException & e)
        {
            /// A batch-level failure skips the per-object response loop below, so record one Delete attempt
            /// per object before rethrowing. Preserve the real HTTP status (as the per-object path below
            /// does) so these failures stay queryable by error_code.
            const auto elapsed = watch.elapsedMicroseconds() / object_batch.size();
            for (const auto & object : object_batch)
                add_log_entry(object, elapsed, static_cast<Int32>(e.StatusCode), e.Message);
            throw;
        }
        catch (...)
        {
            /// Non-Azure failure (e.g. a credential AuthenticationException) carries no HTTP status.
            const auto elapsed = watch.elapsedMicroseconds() / object_batch.size();
            const auto batch_error = getCurrentExceptionMessage(false);
            for (const auto & object : object_batch)
                add_log_entry(object, elapsed, -1, batch_error);
            throw;
        }

        size_t avg_elapsed_us = watch.elapsedMicroseconds() / object_batch.size();
        std::exception_ptr throw_at_end;
        for (const auto [object, deferred_response] : std::views::zip(object_batch, responses))
        {
            try
            {
                deferred_response.GetResponse();
                add_log_entry(object, avg_elapsed_us);

                if (successful_objects)
                    successful_objects->emplace_back(object);
            }
            catch (const Azure::Storage::StorageException & e)
            {
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                {
                    add_log_entry(object, avg_elapsed_us);

                    if (successful_objects)
                        successful_objects->emplace_back(object);
                }
                else
                {
                    add_log_entry(object, avg_elapsed_us, static_cast<Int32>(e.StatusCode), e.Message);

                    if (!throw_at_end)
                    {
                        /// The precondition did not hold: the blob is not the generation the caller
                        /// selected, and it stays in place. Reported the same way as by `removeObjectImpl`.
                        if (!object.etag.empty() && e.StatusCode == Azure::Core::Http::HttpStatusCode::PreconditionFailed)
                            throw_at_end = std::make_exception_ptr(Exception(
                                ErrorCodes::FILE_CHANGED_DURING_READ,
                                "Object {} was not deleted: it changed after it was selected (its `ETag` is no longer {})",
                                object.remote_path, object.etag));
                        else
                            throw_at_end = std::current_exception();
                    }

                    continue;
                }
            }
        }

        if (throw_at_end)
            std::rethrow_exception(throw_at_end);
    }
}

void AzureObjectStorage::removeObjectsIfExist( /// NOLINT
    const StoredObjects & objects,
    StoredObjects * successful_objects)
{
    if (objects.empty())
        return;

    auto client_ptr = client.get();
    auto blob_storage_log = BlobStorageLogWriter::create(name);

    if (isAdlsGen2Endpoint(connection_params.endpoint))
    {
        /// An object that is no longer the generation the caller selected is left in place, but
        /// that must not keep the other objects from being deleted.
        std::exception_ptr throw_at_end;
        for (const auto & object : objects)
        {
            try
            {
                removeObjectImpl(object, client_ptr, /*if_exists=*/ true, blob_storage_log, successful_objects);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::FILE_CHANGED_DURING_READ)
                    throw;
                if (!throw_at_end)
                    throw_at_end = std::current_exception();
            }
        }
        if (throw_at_end)
            std::rethrow_exception(throw_at_end);
        return;
    }

    removeObjectsBatchIfExists(objects, client_ptr, blob_storage_log, successful_objects);
}

static void setAzureBlobTag(
    const std::shared_ptr<const AzureBlobStorage::ContainerClient> & client_ptr,
    const StoredObjects & objects,
    const String & tag_key,
    const String & tag_value,
    StoredObjects * successful_objects)
{
    auto log = getLogger("setAzureBlobTag");
    for (const StoredObject & object : objects)
    {
        const String & blob_name = object.remote_path;

        auto blob_client = client_ptr->GetBlobClient(blob_name);
        auto get_response = blob_client.GetTags();
        auto & tags = get_response.Value;
        const auto tag_iter = tags.find(tag_key);

        if (tag_iter != tags.end() && tag_iter->second == tag_value)
        {
            LOG_TRACE(log, "Azure blob {} skipped as it already had the tag {}={}", blob_name, tag_key, tag_value);
        }
        else
        {
            tags[tag_key] = tag_value;
            blob_client.SetTags(tags);
            LOG_TRACE(log, "Tags of Azure blob {} updated", blob_name);
        }

        if (successful_objects)
            successful_objects->emplace_back(object);
    }
}

void AzureObjectStorage::tagObjects( /// NOLINT
    const StoredObjects & objects,
    const std::string & tag_key,
    const std::string & tag_value,
    StoredObjects * successful_objects)
{
    auto client_ptr = client.get();
    setAzureBlobTag(client_ptr, objects, tag_key, tag_value, successful_objects);
}

ObjectMetadata AzureObjectStorage::getObjectMetadata(const std::string & path, bool) const
{
    auto client_ptr = client.get();
    auto blob_client = client_ptr->GetBlobClient(path);
    auto properties = blob_client.GetProperties().Value;

    ProfileEvents::increment(ProfileEvents::AzureGetProperties);
    if (client_ptr->IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureGetProperties);

    ObjectMetadata result;
    result.size_bytes = properties.BlobSize;
    result.etag = AzureBlobStorage::getETagOrEmpty(properties.ETag);
    if (!properties.Metadata.empty())
    {
        result.attributes.emplace();
        for (const auto & [key, value] : properties.Metadata)
            result.attributes[key] = value;
    }
    result.last_modified = static_cast<std::chrono::system_clock::time_point>(properties.LastModified).time_since_epoch().count();
    return result;
}

std::optional<ObjectMetadata> AzureObjectStorage::tryGetObjectMetadata(const std::string & path, bool with_tags) const
try
{
    return getObjectMetadata(path, with_tags);
}
catch (const Azure::Storage::StorageException & e)
{
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
        return {};
    throw;
}

void AzureObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings &,
    std::optional<ObjectAttributes> object_to_attributes)
{
    auto settings_ptr = settings.get();
    auto client_ptr = client.get();

    /// A source that carries an `ETag` names the generation the caller has seen (a queue copies the
    /// generation it ingested); the copy is pinned to it and transfers that generation or fails.
    /// Its size normally comes from the same listing entry. When the caller knows neither, or
    /// knows the generation but not its size, one `HEAD` supplies what is missing, and the size
    /// and the generation then come from that same `HEAD`, so a read-and-write fallback copies
    /// exactly one generation or fails.
    String src_etag = object_from.etag;
    size_t src_size = object_from.bytes_size;
    if (src_etag.empty() || src_size == StoredObject::UnknownSize)
    {
        auto object_metadata = getObjectMetadata(object_from.remote_path, false);
        /// The `HEAD` was made in order to pin the copy (or to size it, which only means something
        /// for the generation it was measured on), so a `HEAD` that names no generation cannot
        /// deliver what it was made for: the copy would proceed without `If-Match`, and the
        /// read-and-write fallback would be free to stitch two generations of the source together.
        /// An endpoint that omits the header is refused here, the same way the backup and the
        /// `ObjectStorageQueue` paths refuse it, instead of copying an unknown generation. This is
        /// decided before the comparison with a generation the caller carries: an endpoint that
        /// reports nothing has not reported a change either.
        if (object_metadata.etag.empty())
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                "Object {} was not copied: the endpoint reports no `ETag` for it, so the copy cannot "
                "be pinned to the generation of the object that is being copied",
                object_from.remote_path);

        if (!src_etag.empty() && AzureBlobStorage::normalizeETag(object_metadata.etag) != AzureBlobStorage::normalizeETag(src_etag))
            throw Exception(
                ErrorCodes::FILE_CHANGED_DURING_READ,
                "Object {} was not copied: it changed after it was selected (its `ETag` is {} instead of {})",
                object_from.remote_path, object_metadata.etag, src_etag);
        src_etag = object_metadata.etag;
        src_size = object_metadata.size_bytes;
    }

    ProfileEvents::increment(ProfileEvents::AzureCopyObject);
    if (client_ptr->IsClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskAzureCopyObject);
    LOG_TRACE(log, "AzureObjectStorage::copyObject of size {}", src_size);

    auto scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), ThreadName::AZURE_COPY_POOL);

    copyAzureBlobStorageFile(
        client_ptr,
        client_ptr,
        connection_params.getContainer(),
        object_from.remote_path,
        src_size,
        src_etag,
        connection_params.getContainer(),
        object_to.remote_path,
        settings_ptr,
        read_settings,
        object_to_attributes,
        scheduler);
}

void AzureObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    const ApplyNewSettingsOptions & options)
{
    auto new_settings = AzureBlobStorage::getRequestSettings(config, config_prefix, context->getSettingsRef());
    settings.set(std::move(new_settings));

    if (!options.allow_client_change)
        return;

    bool is_client_for_disk = client.get()->IsClientForDisk();

    AzureBlobStorage::ConnectionParams params;
    params.endpoint = AzureBlobStorage::processEndpoint(config, config_prefix);
    params.auth_method = AzureBlobStorage::getAuthMethod(config, config_prefix);
    params.client_options = AzureBlobStorage::getClientOptions(context, context->getSettingsRef(), *settings.get(), is_client_for_disk);

    auto new_client = AzureBlobStorage::getContainerClient(params, /*readonly=*/ true);
    client.set(std::move(new_client));
}

}

#endif
