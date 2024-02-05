#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileCacheSettings.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/**
 * Wraps another object storage and add a caching layer for it.
 */
class CachedObjectStorage final : public IObjectStorage
{
public:
    CachedObjectStorage(ObjectStoragePtr object_storage_, FileCachePtr cache_, const FileCacheSettings & cache_settings_, const String & cache_config_name_);

    std::string getName() const override { return fmt::format("CachedObjectStorage-{}({})", cache_config_name, object_storage->getName()); }

    ObjectStorageType getType() const override { return object_storage->getType(); }

    std::string getCommonKeyPrefix() const override { return object_storage->getCommonKeyPrefix(); }

    std::string getDescription() const override { return object_storage->getDescription(); }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

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

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    String getObjectsNamespace() const override;

    const std::string & getCacheName() const override { return cache_config_name; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) const override;

    bool isRemote() const override { return object_storage->isRemote(); }

    void removeCacheIfExists(const std::string & path_key_for_cache) override;

    bool supportsCache() const override { return true; }

    std::string getUniqueId(const std::string & path) const override { return object_storage->getUniqueId(path); }

    bool isReadOnly() const override { return object_storage->isReadOnly(); }

    bool isWriteOnce() const override { return object_storage->isWriteOnce(); }

    const std::string & getCacheConfigName() const { return cache_config_name; }

    ObjectStoragePtr getWrappedObjectStorage() { return object_storage; }

    bool supportParallelWrite() const override { return object_storage->supportParallelWrite(); }

    const FileCacheSettings & getCacheSettings() const { return cache_settings; }

    static bool canUseReadThroughCache(const ReadSettings & settings);

private:
    FileCacheKey getCacheKey(const std::string & path) const;

    ReadSettings patchSettings(const ReadSettings & read_settings) const override;

    ObjectStoragePtr object_storage;
    FileCachePtr cache;
    FileCacheSettings cache_settings;
    std::string cache_config_name;
    LoggerPtr log;
};

}
