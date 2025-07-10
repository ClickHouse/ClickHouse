#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/MultiVersion.h>
#include <azure/storage/blobs.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class AzureObjectStorage : public IObjectStorage
{
public:
    using ClientPtr = std::unique_ptr<AzureBlobStorage::ContainerClient>;
    using SettingsPtr = std::unique_ptr<AzureBlobStorage::RequestSettings>;

    AzureObjectStorage(
        const String & name_,
        AzureBlobStorage::AuthMethod auth_method,
        ClientPtr && client_,
        SettingsPtr && settings_,
        const String & object_namespace_,
        const String & description_);

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    /// Sanitizer build may crash with max_keys=1; this looks like a false positive.
    ObjectStorageIteratorPtr iterate(const std::string & path_prefix, size_t max_keys) const override;

    std::string getName() const override { return "AzureObjectStorage"; }

    ObjectStorageType getType() const override { return ObjectStorageType::Azure; }

    std::string getRootPrefix() const override { return object_namespace; }

    /// Object keys are unique within the object namespace (container + prefix).
    std::string getCommonKeyPrefix() const override { return ""; }

    std::string getDescription() const override { return description; }

    bool exists(const StoredObject & object) const override;

    AzureBlobStorage::AuthMethod getAzureBlobStorageAuthMethod() const override { return auth_method; }

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

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override {}

    void startup() override {}

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context,
        const ApplyNewSettingsOptions & options) override;

    String getObjectsNamespace() const override { return object_namespace ; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const override;

    bool areObjectKeysRandom() const override { return true; }

    bool isRemote() const override { return true; }

    std::shared_ptr<const AzureBlobStorage::RequestSettings> getSettings() const  { return settings.get(); }
    std::shared_ptr<const AzureBlobStorage::ContainerClient> getAzureBlobStorageClient() const override { return client.get(); }

    bool isReadOnly() const override { return settings.get()->read_only; }

    bool supportParallelWrite() const override { return true; }

private:
    void removeObjectImpl(
        const StoredObject & object,
        const std::shared_ptr<const AzureBlobStorage::ContainerClient> & client_ptr,
        bool if_exists);

    const String name;
    AzureBlobStorage::AuthMethod auth_method;
    /// client used to access the files in the Blob Storage cloud
    MultiVersion<AzureBlobStorage::ContainerClient> client;
    MultiVersion<AzureBlobStorage::RequestSettings> settings;
    const String object_namespace; /// container + prefix

    /// We use source url without container and prefix as description, because in Azure there are no limitations for operations between different containers.
    const String description;

    LoggerPtr log;
};

}

#endif
