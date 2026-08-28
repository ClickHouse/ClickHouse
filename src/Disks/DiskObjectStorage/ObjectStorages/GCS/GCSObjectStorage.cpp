#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>

#include <Interpreters/Context.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/ReadBufferFromGCS.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/list_objects_reader.h>
#include <google/cloud/storage/object_metadata.h>
#include <google/cloud/storage/well_known_parameters.h>

namespace gcs = ::google::cloud::storage;

namespace ProfileEvents
{
    extern const Event GCSGetObjectMetadata;
    extern const Event GCSDeleteObjects;
    extern const Event GCSCopyObject;
    extern const Event DiskGCSGetObjectMetadata;
    extern const Event DiskGCSDeleteObjects;
    extern const Event DiskGCSCopyObject;
}

namespace CurrentMetrics
{
    extern const Metric ObjectStorageGCSThreads;
    extern const Metric ObjectStorageGCSThreadsActive;
    extern const Metric ObjectStorageGCSThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Requests made on behalf of a server-configured disk are counted twice: once in the generic
    /// `GCS*` event and once in the `DiskGCS*` one, so disk traffic can be told apart from traffic
    /// of the `gcs()` table function. This mirrors the `S3*` / `DiskS3*` split.
    void countRequest(ProfileEvents::Event event, ProfileEvents::Event disk_event, bool for_disk, size_t amount = 1)
    {
        ProfileEvents::increment(event, amount);
        if (for_disk)
            ProfileEvents::increment(disk_event, amount);
    }

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
        for (const auto & [key, value] : md.metadata())
            result.attributes.emplace(key, value);
        return result;
    }

    /// The number of objects to request per listing page. A configured `list_object_keys_size` of 0
    /// means "unset"; fall back to the GCS JSON API's own default page size.
    constexpr size_t DEFAULT_LIST_PAGE_SIZE = 1000;

    size_t listPageSize(size_t list_object_keys_size)
    {
        return list_object_keys_size ? list_object_keys_size : DEFAULT_LIST_PAGE_SIZE;
    }

    /// Streams a listing batch by batch (like the S3 and Azure backends) instead of materializing all
    /// matching objects up front, so memory usage and time to the first result scale with the batch
    /// size rather than with the total number of objects under the prefix. `ListObjectsReader` pages
    /// through the listing lazily; the reader and its iterator live on the base class's single
    /// listing thread, which is also where they are first created.
    class GCSIteratorAsync final : public IObjectStorageIteratorAsync
    {
    public:
        GCSIteratorAsync(
            std::shared_ptr<gcs::Client> client_,
            String bucket_,
            String path_prefix_,
            String disk_name_,
            const std::optional<std::string> & start_after_,
            size_t max_list_size_)
            : IObjectStorageIteratorAsync(
                CurrentMetrics::ObjectStorageGCSThreads,
                CurrentMetrics::ObjectStorageGCSThreadsActive,
                CurrentMetrics::ObjectStorageGCSThreadsScheduled,
                ThreadName::GCS_LIST_POOL)
            , client(std::move(client_))
            , bucket(std::move(bucket_))
            , path_prefix(std::move(path_prefix_))
            , disk_name(std::move(disk_name_))
            , start_after(start_after_.has_value() ? *start_after_ : "")
            , batch_size(listPageSize(max_list_size_))
        {
        }

        ~GCSIteratorAsync() override
        {
            /// Stop the listing thread before the reader it iterates is destroyed.
            deactivate();
        }

    private:
        bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
        {
            chassert(batch.empty());

            if (!reader)
            {
                /// `StartOffset` is inclusive while `start_after` is exclusive (matching the S3
                /// backend's ListObjectsV2 semantics), so an object named exactly `start_after`
                /// is skipped below.
                /// The `GCSListObjects` counter is incremented by the transport (see `getGCSClient`),
                /// once per `objects.list` request: the library fetches the later pages of this reader
                /// on its own as the iteration advances, so counting here would see only the first one.
                if (start_after.empty())
                    reader.emplace(client->ListObjects(bucket, gcs::Prefix(path_prefix), gcs::MaxResults(batch_size)));
                else
                    reader.emplace(client->ListObjects(
                        bucket, gcs::Prefix(path_prefix), gcs::StartOffset(start_after), gcs::MaxResults(batch_size)));
                reader_position.emplace(reader->begin());
            }

            batch.reserve(batch_size);
            for (auto & position = *reader_position; position != reader->end(); ++position)
            {
                const auto & item = *position;
                if (!item)
                    throwFromGCSStatus(item.status(),
                        fmt::format("while listing objects in bucket '{}' with prefix '{}' on disk '{}'", bucket, path_prefix, disk_name));

                if (!start_after.empty() && item->name() <= start_after)
                    continue;

                batch.emplace_back(std::make_shared<RelativePathWithMetadata>(item->name(), toObjectMetadata(*item)));

                if (batch.size() >= batch_size)
                {
                    ++position;
                    break;
                }
            }

            return *reader_position != reader->end();
        }

        std::shared_ptr<gcs::Client> client;
        const String bucket;
        const String path_prefix;
        const String disk_name;
        const String start_after;
        const size_t batch_size;

        std::optional<gcs::ListObjectsReader> reader;
        std::optional<gcs::ListObjectsReader::iterator> reader_position;
    };
}

bool GCSObjectStorage::exists(const StoredObject & object) const
{
    auto snapshot = getClientWithSettings();
    countRequest(ProfileEvents::GCSGetObjectMetadata, ProfileEvents::DiskGCSGetObjectMetadata, snapshot->settings.for_disk);
    auto metadata = snapshot->client->GetObjectMetadata(bucket, object.remote_path);
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

    auto snapshot = getClientWithSettings();
    return std::make_unique<ReadBufferFromGCS>(
        snapshot->client,
        bucket,
        object.remote_path,
        patchSettings(read_settings),
        use_external_buffer,
        /* offset */ 0,
        /* read_until_position */ 0,
        restrict_seek,
        file_size,
        expected_generation,
        std::move(blob_storage_log),
        snapshot->settings.for_disk);
}

SmallObjectDataWithMetadata GCSObjectStorage::readSmallObjectAndGetObjectMetadata( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    size_t max_size_bytes,
    std::optional<size_t> read_hint) const
{
    /// The data and the metadata must describe the same object version: the caller uses the returned etag
    /// as the precondition of a later conditional write -- Iceberg's `version-hint.text` compare-and-swap
    /// (`Iceberg::Utils.cpp`) is the reason this method exists -- so a pair stitched together from two
    /// generations would let one writer silently overwrite another's update.
    ///
    /// Unlike `ReadBufferFromS3`, the SDK's `ObjectReadStream` does not expose the generation it served
    /// (its `headers()` accessor is documented as unstable and debug-only), so the metadata cannot come
    /// out of the read itself. Fetch it first, then pin the read to that generation with
    /// `IfGenerationMatch`: a concurrent overwrite in between fails the read instead of returning an
    /// inconsistent pair. That costs one extra `GetObjectMetadata` request, which is what the two-request
    /// shape of the S3 path costs too, and this is a small hint file on a write path.
    auto metadata = getObjectMetadata(object.remote_path, /* with_tags */ false);

    StoredObject pinned = object;
    pinned.etag = metadata.etag;
    pinned.bytes_size = metadata.size_bytes;

    auto buffer = readObject(pinned, read_settings, read_hint);
    SmallObjectDataWithMetadata result;
    WriteBufferFromString out(result.data);
    copyDataMaxBytes(*buffer, out, max_size_bytes);
    out.finalize();
    result.metadata = std::move(metadata);
    return result;
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
    auto snapshot = getClientWithSettings();
    return std::make_unique<WriteBufferFromGCS>(
        snapshot->client,
        bucket,
        object.remote_path,
        buf_size,
        patchSettings(write_settings),
        std::move(blob_storage_log),
        std::move(attributes),
        snapshot->settings.for_disk);
}

void GCSObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    auto snapshot = getClientWithSettings();

    /// `max_keys` bounds the whole listing, `list_object_keys_size` is the page size (like the S3
    /// backend's `MaxKeys`). Never ask for more per page than the caller wants in total.
    size_t page_size = listPageSize(snapshot->settings.list_object_keys_size);
    if (max_keys)
        page_size = std::min(page_size, max_keys);

    size_t count = 0;
    /// Counted per request by the transport, as in `GCSIteratorAsync::getBatchAndCheckNext` above.
    for (auto && item : snapshot->client->ListObjects(bucket, gcs::Prefix(path), gcs::MaxResults(static_cast<Int64>(page_size))))
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
    size_t max_keys,
    bool /* with_tags */,
    const std::optional<std::string> & start_after) const
{
    /// Callers (e.g. the glob source in StorageObjectStorageSource) expect the iterator to enumerate
    /// *all* matching objects, using max_keys only as a batch-size hint. When they give no hint, use
    /// the configured `list_object_keys_size` (the S3 and Azure backends do the same).
    auto snapshot = getClientWithSettings();
    if (!max_keys)
        max_keys = snapshot->settings.list_object_keys_size;
    return std::make_shared<GCSIteratorAsync>(
        snapshot->client, bucket, path_prefix, disk_name, start_after, max_keys);
}

void GCSObjectStorage::removeObjectImpl(
    const StoredObject & object,
    gcs::Client & client_ref,
    const BlobStorageLogWriterPtr & blob_storage_log)
{
    Stopwatch watch;
    countRequest(ProfileEvents::GCSDeleteObjects, ProfileEvents::DiskGCSDeleteObjects, getClientWithSettings()->settings.for_disk);
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
    auto snapshot = getClientWithSettings();
    countRequest(ProfileEvents::GCSGetObjectMetadata, ProfileEvents::DiskGCSGetObjectMetadata, snapshot->settings.for_disk);
    auto metadata = snapshot->client->GetObjectMetadata(bucket, path);
    if (!metadata)
        throwFromGCSStatus(metadata.status(),
            fmt::format("while reading metadata of '{}' in bucket '{}' on disk '{}'", path, bucket, disk_name));
    return toObjectMetadata(*metadata);
}

std::optional<ObjectMetadata> GCSObjectStorage::tryGetObjectMetadata(const std::string & path, bool /*with_tags*/) const
{
    auto snapshot = getClientWithSettings();
    countRequest(ProfileEvents::GCSGetObjectMetadata, ProfileEvents::DiskGCSGetObjectMetadata, snapshot->settings.for_disk);
    auto metadata = snapshot->client->GetObjectMetadata(bucket, path);
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
    auto snapshot = getClientWithSettings();
    auto client_ptr = snapshot->client;
    countRequest(ProfileEvents::GCSCopyObject, ProfileEvents::DiskGCSCopyObject, snapshot->settings.for_disk);

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
    /// Both sides are examined through a single client-with-settings snapshot each, and the rewrite
    /// runs on the client from the source snapshot, so a concurrent `applyNewSettings` cannot pair
    /// the settings that were validated with a client built from different ones.
    auto * dest_gcs = dynamic_cast<GCSObjectStorage *>(&object_storage_to);
    auto source_snapshot = getClientWithSettings();
    if (dest_gcs != nullptr && source_snapshot->settings.describesSameClientAs(dest_gcs->getClientWithSettings()->settings))
    {
        countRequest(ProfileEvents::GCSCopyObject, ProfileEvents::DiskGCSCopyObject, source_snapshot->settings.for_disk);
        google::cloud::StatusOr<gcs::ObjectMetadata> result;
        if (object_to_attributes && !object_to_attributes->empty())
        {
            gcs::ObjectMetadata new_metadata;
            for (const auto & [name, value] : *object_to_attributes)
                new_metadata.upsert_metadata(name, value);
            result = source_snapshot->client->RewriteObjectBlocking(
                bucket, object_from.remote_path, dest_gcs->bucket, object_to.remote_path, gcs::WithObjectMetadata(std::move(new_metadata)));
        }
        else
        {
            result = source_snapshot->client->RewriteObjectBlocking(
                bucket, object_from.remote_path, dest_gcs->bucket, object_to.remote_path);
        }
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
    {
        /// Unlike a configured disk, the table engine/table function has no config section to reload.
        /// Its cached client must nevertheless be rebuilt when the accessing session changes the
        /// server-credentials restriction, otherwise an earlier unrestricted session can leak ADC
        /// access to a later restricted one.
        auto current = getClientWithSettings();
        const bool restricts_now = context->shouldRestrictUserQueryS3Credentials();
        if (current->restricts_server_credentials != restricts_now || options.force_client_rebuild)
        {
            checkGCSCredentialsAllowedInUserQuery(current->settings, context);
            auto new_client = getGCSClient(current->settings, context);
            client_with_settings.set(std::make_unique<const ClientWithSettings>(
                ClientWithSettings{std::move(new_client), current->settings, restricts_now}));
        }
        return;
    }

    auto new_settings = GCSObjectStorageSettings::loadFromConfig(config, config_prefix, context);

    if (new_settings.bucket != bucket)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot change GCS bucket of an existing disk from '{}' to '{}'", bucket, new_settings.bucket);

    /// The client and settings are published together as one atomic snapshot (see the field comment).
    /// When the caller forbids a client change, the current client is carried over into the new
    /// snapshot, paired with the settings it may no longer match — that is the caller's explicit choice.
    std::shared_ptr<gcs::Client> new_client;
    if (options.allow_client_change)
        new_client = getGCSClient(new_settings, context);
    else
        new_client = getClient();

    client_with_settings.set(std::make_unique<const ClientWithSettings>(
        ClientWithSettings{std::move(new_client), std::move(new_settings), false}));
}

ObjectStorageKeyGeneratorPtr GCSObjectStorage::createKeyGenerator() const
{
    if (!key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key generator is not set");
    return key_generator;
}

}

#endif
