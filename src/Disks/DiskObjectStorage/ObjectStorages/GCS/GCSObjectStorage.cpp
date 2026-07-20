#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/ReadBufferFromGCS.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <IO/ReadHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/object_metadata.h>
#include <google/cloud/storage/well_known_parameters.h>

namespace gcs = ::google::cloud::storage;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    time_t timePointToEpochSeconds(std::chrono::system_clock::time_point tp)
    {
        return static_cast<time_t>(std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count());
    }

    ObjectMetadata toObjectMetadata(const gcs::ObjectMetadata & md)
    {
        ObjectMetadata result;
        result.size_bytes = md.size();
        result.last_modified = Poco::Timestamp::fromEpochTime(timePointToEpochSeconds(md.updated()));
        /// The native backend uses the object *generation* as its etag. The JSON API etag is an
        /// opaque string that cannot be fed back into a request precondition, while the generation
        /// is GCS's canonical content-version token: it changes on every overwrite (so it is a valid
        /// cache key wherever an etag is expected) and `readObject` can pin ranged re-reads to it
        /// with `IfGenerationMatch` to detect concurrent overwrites (`s3_validate_etag_on_read`).
        result.etag = std::to_string(md.generation());
        return result;
    }
}

bool GCSObjectStorage::exists(const StoredObject & object) const
{
    auto metadata = getClient()->GetObjectMetadata(bucket, object.remote_path);
    if (metadata)
        return true;
    if (isGCSNotFoundError(metadata.status()))
        return false;
    throwFromGCSStatus(metadata.status(),
        fmt::format("while checking existence of '{}' in bucket '{}'", object.remote_path, bucket));
}

std::unique_ptr<ReadBufferFromFileBase> GCSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    bool use_external_buffer,
    bool restrict_seek) const
{
    /// `bytes_size` may be the UnknownSize sentinel (e.g. object of unknown length); map it to nullopt.
    std::optional<size_t> file_size;
    if (object.bytes_size && object.bytes_size != StoredObject::UnknownSize)
        file_size = object.bytes_size;

    /// A non-empty etag means the caller pinned the read to the object version it saw at LIST/HEAD
    /// time (`s3_validate_etag_on_read`). For this backend the etag is the object generation (see
    /// `toObjectMetadata`), enforced on every read request via `IfGenerationMatch`.
    std::optional<Int64> expected_generation;
    if (!object.etag.empty())
    {
        Int64 generation = 0;
        if (!tryParse(generation, object.etag))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Native GCS read of '{}' got etag '{}' which is not an object generation", object.remote_path, object.etag);
        expected_generation = generation;
    }

    BlobStorageLogWriterPtr blob_storage_log;
    if (read_settings.remote_fs_settings.enable_blob_storage_log)
    {
        blob_storage_log = BlobStorageLogWriter::create(disk_name);
        if (blob_storage_log)
            blob_storage_log->local_path = object.local_path;
    }

    return std::make_unique<ReadBufferFromGCS>(
        getClient(),
        bucket,
        object.remote_path,
        patchSettings(read_settings),
        use_external_buffer,
        /* offset */ 0,
        /* read_until_position */ 0,
        restrict_seek,
        file_size,
        expected_generation,
        std::move(blob_storage_log));
}

std::unique_ptr<WriteBufferFromFileBase> GCSObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "GCS doesn't support append to files");

    auto blob_storage_log = BlobStorageLogWriter::create(disk_name);
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    /// Unlike S3 and Azure, no scheduler is passed to the writer: the google-cloud-cpp write stream
    /// is synchronous, and the SDK handles resumable-upload chunking internally, so there are no
    /// parts to upload in parallel.
    return std::make_unique<WriteBufferFromGCS>(
        getClient(),
        bucket,
        object.remote_path,
        buf_size,
        patchSettings(write_settings),
        std::move(blob_storage_log),
        std::move(attributes));
}

void GCSObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    auto client_ptr = getClient();

    size_t count = 0;
    for (auto && item : client_ptr->ListObjects(bucket, gcs::Prefix(path)))
    {
        if (!item)
            throwFromGCSStatus(item.status(),
                fmt::format("while listing objects in bucket '{}' with prefix '{}' on disk '{}'", bucket, path, disk_name));

        children.emplace_back(std::make_shared<RelativePathWithMetadata>(item->name(), toObjectMetadata(*item)));

        if (max_keys && ++count >= max_keys)
            break;
    }
}

ObjectStorageIteratorPtr GCSObjectStorage::iterate(
    const std::string & path_prefix,
    size_t /* max_keys */,
    bool /* with_tags */,
    const std::optional<std::string> & start_after) const
{
    /// Callers (e.g. the glob source in StorageObjectStorageSource) expect the iterator to enumerate
    /// *all* matching objects, using max_keys only as a page-size hint. To avoid silently truncating
    /// at the first page, enumerate everything eagerly and wrap it in a from-list iterator.
    /// NOTE: this materializes the full listing in memory; a lazy paginating iterator (like S3's) is a
    /// future optimization for buckets with a very large number of objects under one prefix.
    RelativePathsWithMetadata files;
    listObjects(path_prefix, files, 0);

    if (start_after && !start_after->empty())
    {
        /// `start_after` is exclusive, matching the S3 backend's ListObjectsV2 semantics.
        std::erase_if(files, [&](const RelativePathWithMetadataPtr & file) { return file->relative_path <= *start_after; });
    }

    return std::make_shared<ObjectStorageIteratorFromList>(std::move(files));
}

void GCSObjectStorage::removeObjectImpl(
    const StoredObject & object,
    gcs::Client & client_ref,
    const BlobStorageLogWriterPtr & blob_storage_log)
{
    Stopwatch watch;
    auto status = client_ref.DeleteObject(bucket, object.remote_path);
    auto elapsed = watch.elapsedMicroseconds();

    /// Record the delete in `system.blob_storage_log` (like the S3 and Azure backends do),
    /// including tolerated "not found" outcomes, which keep their error code and message.
    if (blob_storage_log)
        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Delete,
            bucket,
            object.remote_path,
            object.local_path,
            object.bytes_size,
            elapsed,
            status.ok() ? 0 : static_cast<Int32>(status.code()),
            status.ok() ? "" : status.message());

    if (!status.ok() && !isGCSNotFoundError(status))
        throwFromGCSStatus(status, fmt::format("while removing '{}' in bucket '{}'", object.remote_path, bucket));
}

void GCSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto client_ptr = getClient();
    auto blob_storage_log = BlobStorageLogWriter::create(disk_name);
    removeObjectImpl(object, *client_ptr, blob_storage_log);
}

void GCSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    /// GCS has no batch-delete API (see https://issuetracker.google.com/issues/162653700), delete one by one.
    auto client_ptr = getClient();
    auto blob_storage_log = BlobStorageLogWriter::create(disk_name);
    for (const auto & object : objects)
        removeObjectImpl(object, *client_ptr, blob_storage_log);
}

ObjectMetadata GCSObjectStorage::getObjectMetadata(const std::string & path, bool /*with_tags*/) const
{
    auto metadata = getClient()->GetObjectMetadata(bucket, path);
    if (!metadata)
        throwFromGCSStatus(metadata.status(),
            fmt::format("while reading metadata of '{}' in bucket '{}' on disk '{}'", path, bucket, disk_name));
    return toObjectMetadata(*metadata);
}

std::optional<ObjectMetadata> GCSObjectStorage::tryGetObjectMetadata(const std::string & path, bool /*with_tags*/) const
{
    auto metadata = getClient()->GetObjectMetadata(bucket, path);
    if (metadata)
        return toObjectMetadata(*metadata);
    if (isGCSNotFoundError(metadata.status()))
        return {};
    throwFromGCSStatus(metadata.status(),
        fmt::format("while reading metadata of '{}' in bucket '{}' on disk '{}'", path, bucket, disk_name));
}

void GCSObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings &,
    const WriteSettings &,
    std::optional<ObjectAttributes> object_to_attributes)
{
    auto client_ptr = getClient();

    google::cloud::StatusOr<gcs::ObjectMetadata> result;
    if (object_to_attributes && !object_to_attributes->empty())
    {
        gcs::ObjectMetadata new_metadata;
        for (const auto & [name, value] : *object_to_attributes)
            new_metadata.upsert_metadata(name, value);
        result = client_ptr->RewriteObjectBlocking(
            bucket, object_from.remote_path, bucket, object_to.remote_path, gcs::WithObjectMetadata(std::move(new_metadata)));
    }
    else
    {
        result = client_ptr->RewriteObjectBlocking(bucket, object_from.remote_path, bucket, object_to.remote_path);
    }

    if (!result)
        throwFromGCSStatus(result.status(),
            fmt::format("while copying '{}' to '{}' in bucket '{}'", object_from.remote_path, object_to.remote_path, bucket));
}

void GCSObjectStorage::copyObjectToAnotherObjectStorage( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    /// Server-side rewrite (`RewriteObject`) copies between two buckets through a single client, so it
    /// is valid only when the destination is a native GCS storage that targets the same endpoint with
    /// the same credentials as this one, i.e. genuinely shares this client. Without that check the
    /// rewrite would always run on this storage's endpoint and identity: with a different endpoint a
    /// same-named bucket could be rewritten on the wrong backend and we would return success without
    /// ever writing the real destination; on the same endpoint with different credentials the copy
    /// would be authenticated as the source instead of the destination. In those cases fall back to a
    /// buffer copy, which reads through this client and writes through the destination's own client.
    if (auto * dest_gcs = dynamic_cast<GCSObjectStorage *>(&object_storage_to);
        dest_gcs != nullptr && settings.describesSameClientAs(dest_gcs->settings))
    {
        auto client_ptr = getClient();
        auto result = client_ptr->RewriteObjectBlocking(
            bucket, object_from.remote_path, dest_gcs->bucket, object_to.remote_path);
        if (result)
            return;
        LOG_WARNING(log, "GCS server-side copy from bucket {} to bucket {} failed ({}), falling back to buffer copy",
            bucket, dest_gcs->bucket, result.status().message());
    }

    IObjectStorage::copyObjectToAnotherObjectStorage(
        object_from, object_to, read_settings, write_settings, object_storage_to, object_to_attributes);
}

void GCSObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    const ApplyNewSettingsOptions & options)
{
    /// The object storage table engine and table function (e.g. `gcs(...)`) construct the storage with
    /// all settings already derived from the URL and query arguments; there is no disk configuration
    /// section to reload from. Only the `object_storage_type = gcs` disk path has an `endpoint` entry
    /// under `config_prefix`. Without this guard `loadFromConfig` would read a non-existent
    /// `<config_prefix>.endpoint` key (for the table function it is `gcs..endpoint`) and throw.
    if (!config.has(config_prefix + ".endpoint"))
        return;

    auto new_settings = GCSObjectStorageSettings::loadFromConfig(config, config_prefix, context);

    if (new_settings.bucket != bucket)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot change GCS bucket of an existing disk from '{}' to '{}'", bucket, new_settings.bucket);

    if (options.allow_client_change)
    {
        auto new_client = getGCSClient(new_settings, context);
        std::lock_guard lock(client_mutex);
        client = std::move(new_client);
    }

    settings = std::move(new_settings);
}

ObjectStorageKeyGeneratorPtr GCSObjectStorage::createKeyGenerator() const
{
    if (!key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key generator is not set");
    return key_generator;
}

}

#endif
