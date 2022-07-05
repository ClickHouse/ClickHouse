#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/IFileCache.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class CachedObjectStorage : public IObjectStorage
{
public:
    CachedObjectStorage(ObjectStoragePtr object_storage_, FileCachePtr cache_);

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
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    void listPrefix(const std::string & path, RelativePathsWithSize & children) const override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    String getObjectsNamespace() const override;

    const String & getCacheBasePath() const override { return cache->getBasePath(); }

    std::string generateBlobNameForPath(const std::string & path) override;

    bool isRemote() const override { return object_storage->isRemote(); }

    void removeCacheIfExists(const std::string & cache_hint) override;

private:
    IFileCache::Key getCacheKey(const std::string & path) const;
    String getCachePath(const std::string & path) const;
    ReadSettings getReadSettingsForCache(const ReadSettings & read_settings) const;

    ObjectStoragePtr object_storage;
    FileCachePtr cache;
    Poco::Logger * log;
};

}
