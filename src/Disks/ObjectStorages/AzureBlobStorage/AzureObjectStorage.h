#pragma once
#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/getRandomASCIIString.h>


namespace DB
{

struct AzureObjectStorageSettings
{
    AzureObjectStorageSettings(
        uint64_t max_single_part_upload_size_,
        uint64_t min_bytes_for_seek_,
        int max_single_read_retries_,
        int max_single_download_retries_)
        : max_single_part_upload_size(max_single_part_upload_size_)
        , min_bytes_for_seek(min_bytes_for_seek_)
        , max_single_read_retries(max_single_read_retries_)
        , max_single_download_retries(max_single_download_retries_)
    {
    }

    size_t max_single_part_upload_size; /// NOTE: on 32-bit machines it will be at most 4GB, but size_t is also used in BufferBase for offset
    uint64_t min_bytes_for_seek;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
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
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listPrefix(const std::string & path, RelativePathsWithSize & children) const override;

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
};

}

#endif
