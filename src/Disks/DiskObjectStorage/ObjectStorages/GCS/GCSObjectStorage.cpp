#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/ReadBufferFromGCS.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>
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
        result.etag = md.etag();
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

    return std::make_unique<ReadBufferFromGCS>(
        getClient(),
        bucket,
        object.remote_path,
        patchSettings(read_settings),
        use_external_buffer,
        /* offset */ 0,
        /* read_until_position */ 0,
        restrict_seek,
        file_size);
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

    return std::make_unique<WriteBufferFromGCS>(
        getClient(),
        bucket,
        object.remote_path,
        buf_size,
        patchSettings(write_settings),
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

void GCSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto status = getClient()->DeleteObject(bucket, object.remote_path);
    if (!status.ok() && !isGCSNotFoundError(status))
        throwFromGCSStatus(status, fmt::format("while removing '{}' in bucket '{}'", object.remote_path, bucket));
}

void GCSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    /// GCS has no batch-delete API (see https://issuetracker.google.com/issues/162653700), delete one by one.
    auto client_ptr = getClient();
    for (const auto & object : objects)
    {
        auto status = client_ptr->DeleteObject(bucket, object.remote_path);
        if (!status.ok() && !isGCSNotFoundError(status))
            throwFromGCSStatus(status, fmt::format("while removing '{}' in bucket '{}'", object.remote_path, bucket));
    }
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
    /// Server-side rewrite is possible only within the same GCS project/client. If the destination
    /// is another native GCS storage sharing this client, use RewriteObject across buckets.
    if (auto * dest_gcs = dynamic_cast<GCSObjectStorage *>(&object_storage_to))
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
    auto new_settings = GCSObjectStorageSettings::loadFromConfig(config, config_prefix, context);

    if (new_settings.bucket != bucket)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot change GCS bucket of an existing disk from '{}' to '{}'", bucket, new_settings.bucket);

    if (options.allow_client_change)
    {
        auto new_client = getGCSClient(new_settings);
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
