#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class BackupObjectStorage final : public IObjectStorage
{
public:
    BackupObjectStorage(ObjectStoragePtr object_storage_, const std::string & backup_base_path_, const std::string & backup_config_name_);

    ObjectStorageType getType() const override { return object_storage->getType(); }

    std::string getCommonKeyPrefix() const override { return object_storage->getCommonKeyPrefix(); }

    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    bool exists(const StoredObject & object) const override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const override;

    std::string getDescription() const override { return object_storage->getDescription(); }

    std::string getName() const override { return fmt::format("BackupObjectStorage-{}({})", backup_config_name, object_storage->getName()); }

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override
    {
        return object_storage->readObject(object, read_settings, read_hint, file_size);
    }

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override
    {
        return object_storage->readObjects(objects, read_settings, read_hint, file_size);
    }

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override
    {
        return object_storage->writeObject(object, mode, attributes, buf_size, write_settings);
    }

    virtual void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        object_storage->copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
    }

    virtual void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override
    {
        object_storage->copyObjectToAnotherObjectStorage(
            object_from, object_to, read_settings, write_settings, object_storage_to, object_to_attributes);
    }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override
    {
        return object_storage->cloneObjectStorage(new_namespace, config, config_prefix, context);
    }

    ObjectMetadata getObjectMetadata(const std::string & path) const override { return object_storage->getObjectMetadata(path); }

    void shutdown() override { object_storage->shutdown(); }

    void startup() override { object_storage->startup(); }

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) override
    {
        object_storage->applyNewSettings(config, config_prefix, context);
    }

    String getObjectsNamespace() const override { return object_storage->getObjectsNamespace(); }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) const override { return object_storage->generateObjectKeyForPath(path); }

    bool isRemote() const override { return object_storage->isRemote(); }

    // This function is part of IObjectStorage but only used in CachedObjectStorage
    // without any delegation, it probably shouldn't be part of the interface.
    void removeCacheIfExists(const std::string & /*path_key_for_cache*/) override {}

    bool supportsCache() const override { return false; }

    std::string getUniqueId(const std::string & path) const override { return object_storage->getUniqueId(path); }

    bool isReadOnly() const override { return object_storage->isReadOnly(); }

    bool isWriteOnce() const override { return object_storage->isWriteOnce(); }

    std::optional<std::string> getLayerName() const override { return {backup_config_name}; }

    ObjectStoragePtr getWrappedObjectStorage() override { return object_storage; }

    bool supportParallelWrite() const override { return object_storage->supportParallelWrite(); }

    ReadSettings patchSettings(const ReadSettings & read_settings) const override { return read_settings; }

    WriteSettings patchSettings(const WriteSettings & write_settings) const override { return write_settings; }

private:
    bool isSoftDeleted(const std::string & object_path) const;

    std::string getRemovedMarkerPath(const std::string & object_path) const;

    void removeObjectImpl(const std::string & object_path) const;

    ObjectStoragePtr object_storage;
    std::string backup_base_path;
    std::string backup_config_name;
    Poco::Logger * log;
};

}
