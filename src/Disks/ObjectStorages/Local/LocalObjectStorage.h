#pragma once

#include "config.h"

#include <Disks/ObjectStorages/IObjectStorage.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/// Treat local disk as an object storage (for interface compatibility).
class LocalObjectStorage : public IObjectStorage
{
public:
    explicit LocalObjectStorage(String key_prefix_);

    std::string getName() const override { return "LocalObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Local; }

    std::string getCommonKeyPrefix() const override { return key_prefix; }

    std::string getDescription() const override { return description; }

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

    void removeObjects(const StoredObjects &  objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    String getObjectsNamespace() const override { return ""; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) const override;

    bool isRemote() const override { return false; }

    ReadSettings patchSettings(const ReadSettings & read_settings) const override;

private:
    String key_prefix;
    LoggerPtr log;
    std::string description;
};

}
