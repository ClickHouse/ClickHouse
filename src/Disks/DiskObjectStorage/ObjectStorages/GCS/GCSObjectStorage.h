#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/GCS/GCSClient.h>
#include <Common/BlobStorageLogWriter.h>

#include <functional>
#include <memory>
#include <string_view>
namespace DB
{

struct GCSObjectStorageSettings
{
    using BlobStorageLogWriterFactory = std::function<BlobStorageLogWriterPtr(const String &)>;

    String disk_name;
    String bucket;
    String key_prefix;
    String description;
    bool read_only = false;
    GCS::ClientSettings client_settings;
    BlobStorageLogWriterFactory blob_storage_log_writer_factory;
};

class GCSObjectStorage final : public IObjectStorage
{
public:
#if USE_GOOGLE_CLOUD
    GCSObjectStorage(GCSObjectStorageSettings settings_, std::shared_ptr<GCS::Client> client_);
#else
    explicit GCSObjectStorage(GCSObjectStorageSettings settings_);
#endif

    std::string getName() const override { return "GCS"; }
    std::string getDiskName() const override { return settings.disk_name; }
    ObjectStorageType getType() const override { return ObjectStorageType::GCS; }
    std::string getRootPrefix() const override { return settings.bucket; }
    std::string getCommonKeyPrefix() const override { return settings.key_prefix; }
    std::string getDescription() const override { return settings.description; }
    bool isRemote() const override { return true; }
    bool isReadOnly() const override { return settings.read_only; }
    String getObjectsNamespace() const override { return settings.bucket; }

    bool exists(const StoredObject & object) const override;
    std::unique_ptr<ReadBufferFromFileBase>
    readObject(const StoredObject & object, const ReadSettings & read_settings, std::optional<size_t> read_hint = {}) const override;
    std::unique_ptr<WriteBufferFromFileBase> writeObject(
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;
    void removeObjectIfExists(const StoredObject & object) override;
    void removeObjectsIfExist(const StoredObjects & objects) override;
    void copyObject(
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;
    void copyObjectToAnotherObjectStorage(
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;
    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;
    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;
    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;
    ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix, size_t max_keys, bool with_tags, const std::optional<std::string> & start_after) const override;

    void shutdown() override { }
    void startup() override { }
    bool supportParallelWrite() const override { return true; }
    ReadSettings patchSettings(const ReadSettings & read_settings) const override;
    WriteSettings patchSettings(const WriteSettings & write_settings) const override;

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

private:
    BlobStorageLogWriterPtr createBlobStorageLogWriter() const;
    void removeObjectIfExistsImpl(const StoredObject & object, const BlobStorageLogWriterPtr & blob_storage_log) const;
#if USE_GOOGLE_CLOUD
    bool isCompatibleForNativeRewriteFrom(const GCSObjectStorage & source_storage) const;
    void rewriteObjectFromGCS(
        const GCSObjectStorage & source_storage,
        const StoredObject & object_from,
        const StoredObject & object_to,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes) const;
#endif

    [[noreturn]] void throwNotImplemented(std::string_view operation) const;
    GCSObjectStorageSettings settings;
#if USE_GOOGLE_CLOUD
    std::shared_ptr<GCS::Client> client;
#endif
};

GCSObjectStorageSettings getGCSObjectStorageSettings(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context);

ObjectStoragePtr createGCSObjectStorage(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context,
    bool skip_access_check);

}
