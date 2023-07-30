#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/MultiVersion.h>
#include <azure/storage/blobs.hpp>

namespace Poco
{
class Logger;
}

namespace DB
{

struct AzureObjectStorageSettings
{
    AzureObjectStorageSettings(
        uint64_t max_single_part_upload_size_,
        uint64_t min_bytes_for_seek_,
        int max_single_read_retries_,
        int max_single_download_retries_,
        int list_object_keys_size_)
        : max_single_part_upload_size(max_single_part_upload_size_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , max_single_read_retries(max_single_read_retries_)
        , max_single_download_retries(max_single_download_retries_)
        , list_object_keys_size(list_object_keys_size_)
    {
    }

    AzureObjectStorageSettings() = default;

    size_t max_single_part_upload_size = 100 * 1024 * 1024; /// NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    uint64_t min_bytes_for_seek = 1024 * 1024;
    size_t max_single_read_retries = 3;
    size_t max_single_download_retries = 3;
    int list_object_keys_size = 1000;
};

using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

class AzureObjectStorage : public IObjectStorage
{
public:

    using SettingsPtr = std::unique_ptr<AzureObjectStorageSettings>;

    AzureObjectStorage(
        const String & name_,
        AzureClientPtr && client_,
        SettingsPtr && settings_);

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const override;

    ObjectStorageIteratorPtr iterate(const std::string & path_prefix) const override;

    DataSourceDescription getDataSourceDescription() const override { return data_source_description; }

    std::string getName() const override { return "AzureObjectStorage"; }

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

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override {}

    void startup() override {}

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

    std::string generateBlobNameForPath(const std::string & path) override;

    bool isRemote() const override { return true; }

private:
    const String name;
    /// client used to access the files in the Blob Storage cloud
    MultiVersion<Azure::Storage::Blobs::BlobContainerClient> client;
    MultiVersion<AzureObjectStorageSettings> settings;

    Poco::Logger * log;

    DataSourceDescription data_source_description;
};

}

#endif
