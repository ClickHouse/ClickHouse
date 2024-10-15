#pragma once

#include "config.h"

#if USE_SSL

#    include <Disks/IDisk.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <IO/FileEncryptionCommon.h>
#    include <Interpreters/Cache/FileCache.h>

namespace Poco
{
class Logger;
}

namespace DB
{

struct EncryptedObjectStorageSettings
{
    DiskPtr wrapped_disk;
    String current_key;
    UInt128 current_key_fingerprint;
    FileEncryption::Algorithm current_algorithm;
    std::unordered_map<UInt128 /* fingerprint */, String /* key */> all_keys;
    String findKeyByFingerprint(UInt128 key_fingerprint, const String & path_for_logs) const;
    FileCachePtr header_cache;
    bool cache_header_on_write = false;
};

/**
 * Wraps another object storage and add a caching layer for it.
 */
class EncryptedObjectStorage final : public IObjectStorage
{
public:
    EncryptedObjectStorage(
        ObjectStoragePtr object_storage_, EncryptedObjectStorageSettingsPtr enc_settings_, const String & enc_config_name_);

    ObjectStorageType getType() const override { return object_storage->getType(); }

    std::string getName() const override
    {
        return fmt::format("EncryptedObjectStorage-{}({})", enc_config_name, object_storage->getName());
    }

    std::string getCommonKeyPrefix() const override { return object_storage->getCommonKeyPrefix(); }

    std::string getDescription() const override { return object_storage->getDescription(); }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
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

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /*config_prefix*/,
        ContextPtr /* context */,
        const ApplyNewSettingsOptions & /* options */) override;

    String getObjectsNamespace() const override;

    bool isRemote() const override { return object_storage->isRemote(); }

    std::string getUniqueId(const std::string & path) const override { return object_storage->getUniqueId(path); }

    bool isReadOnly() const override { return object_storage->isReadOnly(); }

    bool isWriteOnce() const override { return object_storage->isWriteOnce(); }

    ObjectStoragePtr getWrappedObjectStorage() override { return object_storage; }

    bool supportParallelWrite() const override { return object_storage->supportParallelWrite(); }

    const EncryptedObjectStorageSettingsPtr & getEncryptionSettings() const { return enc_settings; }

    const std::string & getLayerName() const override { return enc_config_name; }
    bool supportsOverlays() const override { return true; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const override;

private:
    ReadSettings patchSettings(const ReadSettings & read_settings) const override;
    void removeCacheIfExists(const std::string & path) override;

    ObjectStoragePtr object_storage;
    EncryptedObjectStorageSettingsPtr enc_settings;
    const std::string enc_config_name;
    Poco::Logger * log;
};

}

#endif
