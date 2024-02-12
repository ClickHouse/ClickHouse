#pragma once
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/ObjectStorage/StorageObejctStorageConfiguration.h>

namespace DB
{
class BackupFactory;

class StorageAzureBlobConfiguration : public StorageObjectStorageConfiguration
{
    friend class BackupReaderAzureBlobStorage;
    friend class BackupWriterAzureBlobStorage;
    friend void registerBackupEngineAzureBlobStorage(BackupFactory & factory);

public:
    StorageAzureBlobConfiguration() = default;
    StorageAzureBlobConfiguration(const StorageAzureBlobConfiguration & other);

    Path getPath() const override { return blob_path; }
    void setPath(const Path & path) override { blob_path = path; }

    const Paths & getPaths() const override { return blobs_paths; }
    Paths & getPaths() override { return blobs_paths; }

    String getDataSourceDescription() override { return fs::path(connection_url) / container; }
    String getNamespace() const override { return container; }

    void check(ContextPtr context) const override;
    StorageObjectStorageConfigurationPtr clone() override;
    ObjectStoragePtr createOrUpdateObjectStorage(ContextPtr context, bool is_readonly = true) override; /// NOLINT

    void fromNamedCollection(const NamedCollection & collection) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    static void addStructureToArgs(ASTs & args, const String & structure, ContextPtr context);

protected:
    using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
    using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

    std::string connection_url;
    bool is_connection_string;

    std::optional<std::string> account_name;
    std::optional<std::string> account_key;

    std::string container;
    std::string blob_path;
    std::vector<String> blobs_paths;

    AzureClientPtr createClient(bool is_read_only);
    AzureObjectStorage::SettingsPtr createSettings(ContextPtr local_context);
};

}
