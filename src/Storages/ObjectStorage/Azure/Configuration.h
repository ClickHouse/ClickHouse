#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <filesystem>

namespace DB
{
class BackupFactory;

class StorageAzureConfiguration : public StorageObjectStorage::Configuration
{
    friend class BackupReaderAzureBlobStorage;
    friend class BackupWriterAzureBlobStorage;
    friend void registerBackupEngineAzureBlobStorage(BackupFactory & factory);

public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto type_name = "azure";
    static constexpr auto engine_name = "Azure";

    StorageAzureConfiguration() = default;
    StorageAzureConfiguration(const StorageAzureConfiguration & other);

    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

    Path getPath() const override { return blob_path; }
    void setPath(const Path & path) override { blob_path = path; }

    const Paths & getPaths() const override { return blobs_paths; }
    void setPaths(const Paths & paths) override { blobs_paths = paths; }

    String getNamespace() const override { return container; }
    String getDataSourceDescription() const override { return std::filesystem::path(connection_url) / container; }
    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const override;

    void check(ContextPtr context) const override;
    ConfigurationPtr clone() override { return std::make_shared<StorageAzureConfiguration>(*this); }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgs(
        ASTs & args,
        const String & structure_,
        const String & format_,
        ContextPtr context) override;

protected:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

    using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
    using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

    std::string connection_url;
    bool is_connection_string;

    std::optional<std::string> account_name;
    std::optional<std::string> account_key;

    std::string container;
    std::string blob_path;
    std::vector<String> blobs_paths;

    AzureClientPtr createClient(bool is_read_only, bool attempt_to_create_container);
    AzureObjectStorage::SettingsPtr createSettings(ContextPtr local_context);
    Poco::URI getConnectionURL() const;
};

}

#endif
