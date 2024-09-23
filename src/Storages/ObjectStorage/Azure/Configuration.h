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
    /// All possible signatures for Azure engine with structure argument (for example for azureBlobStorage table function).
    static constexpr auto max_number_of_arguments_with_structure = 8;
    static constexpr auto signatures_with_structure =
        " - connection_string, container_name, blobpath\n"
        " - connection_string, container_name, blobpath, structure \n"
        " - connection_string, container_name, blobpath, format \n"
        " - connection_string, container_name, blobpath, format, compression \n"
        " - connection_string, container_name, blobpath, format, compression, structure \n"
        " - storage_account_url, container_name, blobpath, account_name, account_key\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, structure\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure\n";

    /// All possible signatures for Azure engine without structure argument (for example for AzureBlobStorage table engine).
    static constexpr auto max_number_of_arguments_without_structure = 7;
    static constexpr auto signatures_without_structure =
        " - connection_string, container_name, blobpath\n"
        " - connection_string, container_name, blobpath, format \n"
        " - connection_string, container_name, blobpath, format, compression \n"
        " - storage_account_url, container_name, blobpath, account_name, account_key\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n";

    StorageAzureConfiguration() = default;
    StorageAzureConfiguration(const StorageAzureConfiguration & other);

    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

    std::string getSignatures(bool with_structure = true) const { return with_structure ? signatures_with_structure : signatures_without_structure; }
    size_t getMaxNumberOfArguments(bool with_structure = true) const { return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure; }

    Path getPath() const override { return blob_path; }
    void setPath(const Path & path) override { blob_path = path; }

    const Paths & getPaths() const override { return blobs_paths; }
    void setPaths(const Paths & paths) override { blobs_paths = paths; }

    String getNamespace() const override { return connection_params.getContainer(); }
    String getDataSourceDescription() const override { return std::filesystem::path(connection_params.getConnectionURL()) / connection_params.getContainer(); }
    StorageObjectStorage::QuerySettings getQuerySettings(const ContextPtr &) const override;

    void check(ContextPtr context) const override;
    ConfigurationPtr clone() override { return std::make_shared<StorageAzureConfiguration>(*this); }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure_,
        const String & format_,
        ContextPtr context) override;

protected:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

    std::string blob_path;
    std::vector<String> blobs_paths;
    AzureBlobStorage::ConnectionParams connection_params;
};

}

#endif
