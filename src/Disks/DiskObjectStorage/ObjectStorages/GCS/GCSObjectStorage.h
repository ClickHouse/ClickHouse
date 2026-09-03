#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <memory>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/client.h>

namespace DB
{

/// Native Google Cloud Storage object storage, built on the google-cloud-cpp storage client
/// (the native JSON API), as opposed to the S3-compatibility path used by S3ObjectStorage.
class GCSObjectStorage : public IObjectStorage
{
public:
    GCSObjectStorage(
        std::unique_ptr<google::cloud::storage::Client> client_,
        GCSObjectStorageSettings settings_,
        String description_,
        ObjectStorageKeyGeneratorPtr key_generator_,
        String disk_name_)
        : bucket(settings_.bucket)
        , key_prefix(settings_.key_prefix)
        , description(std::move(description_))
        , disk_name(std::move(disk_name_))
        , client_with_settings(std::make_unique<const ClientWithSettings>(
              ClientWithSettings{std::move(client_), std::move(settings_)}))
        , key_generator(std::move(key_generator_))
        , log(getLogger("GCSObjectStorage"))
    {
    }

    std::string getName() const override { return "GCS"; }
    std::string getDiskName() const override { return disk_name; }
    std::string getCommonKeyPrefix() const override { return key_prefix; }
    std::string getDescription() const override { return description; }
    ObjectStorageType getType() const override { return ObjectStorageType::GCS; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        bool use_external_buffer = false,
        bool restrict_seek = false) const override;

    SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        size_t max_size_bytes,
        std::optional<size_t> read_hint = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix,
        size_t max_keys,
        bool with_tags,
        const std::optional<std::string> & start_after) const override;

    void removeObjectIfExists(const StoredObject & object) override;
    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;
    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override { }
    void startup() override { }

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context,
        const ApplyNewSettingsOptions & options) override;

    String getObjectsNamespace() const override { return bucket; }

    bool isRemote() const override { return true; }
    bool supportParallelWrite() const override { return true; }
    bool isReadOnly() const override { return getClientWithSettings()->settings.read_only; }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    /// Aliases the immutable client/settings snapshot, so the returned pointer keeps the settings
    /// it points into alive even if `applyNewSettings` swaps the snapshot concurrently.
    std::shared_ptr<const GCSObjectStorageSettings> getGCSObjectStorageSettings() const override
    {
        auto snapshot = getClientWithSettings();
        return {snapshot, &snapshot->settings};
    }

private:
    /// The client and the settings it was built from are published as one immutable snapshot: a
    /// consumer that validates something against the settings (e.g. `describesSameClientAs` in
    /// `copyObjectToAnotherObjectStorage`) and then issues requests must do both through the same
    /// snapshot. If they were separate fields, a disk config reload (`applyNewSettings`) could
    /// replace the client between the two reads, so requests would run on a client that no longer
    /// matches the settings the consumer just checked.
    struct ClientWithSettings
    {
        std::shared_ptr<google::cloud::storage::Client> client;
        GCSObjectStorageSettings settings;
        /// Only non-disk GCS storages depend on this session-scoped security mode.
        bool restricts_server_credentials = false;
    };

    using ClientWithSettingsPtr = MultiVersion<ClientWithSettings>::Version;

    ClientWithSettingsPtr getClientWithSettings() const { return client_with_settings.get(); }

    std::shared_ptr<google::cloud::storage::Client> getClient() const { return getClientWithSettings()->client; }

    /// Deletes one object (tolerating "not found") and records a Delete event in `system.blob_storage_log`.
    void removeObjectImpl(
        const StoredObject & object,
        google::cloud::storage::Client & client_ref,
        const BlobStorageLogWriterPtr & blob_storage_log);

    const String bucket;
    const String key_prefix;
    const String description;
    std::string disk_name;

    MultiVersion<ClientWithSettings> client_with_settings;
    const ObjectStorageKeyGeneratorPtr key_generator;
    LoggerPtr log;
};

}

#endif
