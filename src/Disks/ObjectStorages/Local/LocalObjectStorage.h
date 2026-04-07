#pragma once

#include "config.h"

#include <Disks/ObjectStorages/IObjectStorage.h>


namespace Poco
{
    class Logger;
}

namespace DB
{

struct LocalObjectStorageSettings
{
    LocalObjectStorageSettings(String key_prefix_, bool read_only_)
        : key_prefix(key_prefix_), read_only(read_only_)
    {
    }

    String key_prefix;
    bool read_only = false;
};

/// Treat local disk as an object storage (for interface compatibility).
class LocalObjectStorage : public IObjectStorage
{
public:
    explicit LocalObjectStorage(LocalObjectStorageSettings settings_);

    std::string getName() const override { return "LocalObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Local; }

    std::string getCommonKeyPrefix() const override { return settings.key_prefix; }

    std::string getDescription() const override { return description; }

    bool isReadOnly() const override { return settings.read_only; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    bool existsOrHasAnyChild(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    String getObjectsNamespace() const override { return ""; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const override;

    bool areObjectKeysRandom() const override { return true; }

    bool isRemote() const override { return false; }

    ReadSettings patchSettings(const ReadSettings & read_settings) const override;

private:
    void removeObject(const StoredObject & object) const;
    void removeObjects(const StoredObjects &  objects) const;

    void throwIfReadonly() const;

    LocalObjectStorageSettings settings;
    LoggerPtr log;
    std::string description;
};

}
