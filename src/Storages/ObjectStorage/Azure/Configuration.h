#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <filesystem>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class BackupFactory;

class StorageAzureConfiguration : public StorageObjectStorageConfiguration
{
    friend class BackupReaderAzureBlobStorage;
    friend class BackupWriterAzureBlobStorage;
    friend void registerBackupEngineAzureBlobStorage(BackupFactory & factory);

public:
    static constexpr auto type = ObjectStorageType::Azure;
    static constexpr auto type_name = "azure";
    static constexpr auto engine_name = "Azure";
    /// All possible signatures for Azure engine with structure argument (for example for azureBlobStorage table function).
    static constexpr auto max_number_of_arguments_with_structure = 10;
    static constexpr auto signatures_with_structure =
        " - connection_string, container_name, blobpath\n"
        " - connection_string, container_name, blobpath, structure \n"
        " - connection_string, container_name, blobpath, format \n"
        " - connection_string, container_name, blobpath, format, compression \n"
        " - connection_string, container_name, blobpath, format, compression, partition_strategy \n"
        " - connection_string, container_name, blobpath, format, compression, structure \n"
        " - connection_string, container_name, blobpath, format, compression, partition_strategy, structure \n"
        " - connection_string, container_name, blobpath, format, compression, partition_strategy, partition_columns_in_data_file \n"
        " - connection_string, container_name, blobpath, format, compression, partition_strategy, partition_columns_in_data_file, structure \n"
        " - storage_account_url, container_name, blobpath, account_name, account_key\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, structure\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, structure\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file, structure\n";

    /// All possible signatures for Azure engine without structure argument (for example for AzureBlobStorage table engine).
    static constexpr auto max_number_of_arguments_without_structure = 9;
    static constexpr auto signatures_without_structure =
        " - connection_string, container_name, blobpath\n"
        " - connection_string, container_name, blobpath, format \n"
        " - connection_string, container_name, blobpath, format, compression \n"
        " - storage_account_url, container_name, blobpath, account_name, account_key\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file\n";

    StorageAzureConfiguration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

    std::string getSignatures(bool with_structure = true) const { return with_structure ? signatures_with_structure : signatures_without_structure; }
    size_t getMaxNumberOfArguments(bool with_structure = true) const { return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure; }

    Path getRawPath() const override { return blob_path; }
    const String & getRawURI() const override { return blob_path.path; }

    const Paths & getPaths() const override { return blobs_paths; }
    void setPaths(const Paths & paths) override { blobs_paths = paths; }

    String getNamespace() const override { return connection_params.getContainer(); }
    String getDataSourceDescription() const override { return std::filesystem::path(connection_params.getConnectionURL()) / connection_params.getContainer(); }
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr &) const override;

    void check(ContextPtr context) override;

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args,
        const String & structure_,
        const String & format_,
        ContextPtr context,
        bool with_structure) override;

    void setInitializationAsOneLake(const String & client_id_, const String & client_secret_, const String & tenant_id_)
    {
        onelake_client_id = client_id_;
        onelake_client_secret = client_secret_;
        onelake_tenant_id = tenant_id_;
    }

protected:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;
    ASTPtr extractExtraCredentials(ASTs & args);
    bool collectCredentials(ASTPtr maybe_credentials, std::optional<String> & client_id, std::optional<String> & tenant_id, ContextPtr local_context);
    void initializeForOneLake(
        ASTs & args,
        ContextPtr context);

    void fillBlobsFromURLCommon(String & connection_url, const String & suffix, const String & full_suffix);

    Path blob_path;
    Paths blobs_paths;
    AzureBlobStorage::ConnectionParams connection_params;
    DiskPtr disk;

    String onelake_client_id;
    String onelake_client_secret;
    String onelake_tenant_id;
};

}

#endif
