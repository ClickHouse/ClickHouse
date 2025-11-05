#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <filesystem>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>


namespace DB
{
class BackupFactory;

struct AzureStorageParsableArguments : private StorageParsableArguments
{
    friend class StorageAzureConfiguration;
    static constexpr auto max_number_of_arguments_with_structure = 10;
    static constexpr auto signatures_with_structure
        = " - connection_string, container_name, blobpath, structure \n"
          " - connection_string, container_name, blobpath, format, compression, structure \n"
          " - connection_string, container_name, blobpath, format, compression, partition_strategy, structure \n"
          " - connection_string, container_name, blobpath, format, compression, partition_strategy, partition_columns_in_data_file, "
          "structure \n"
          " - storage_account_url, container_name, blobpath, account_name, account_key, structure\n"
          " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure\n"
          " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, "
          "structure\n"
          " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, "
          "partition_columns_in_data_file, structure\n";

    /// All possible signatures for Azure engine without structure argument (for example for AzureBlobStorage table engine).
    static constexpr auto max_number_of_arguments_without_structure = max_number_of_arguments_with_structure - 1;
    static constexpr auto signatures_without_structure =
        " - connection_string, container_name, blobpath\n"
        " - connection_string, container_name, blobpath, format \n"
        " - connection_string, container_name, blobpath, format, compression \n"
        " - storage_account_url, container_name, blobpath, account_name, account_key\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy\n"
        " - storage_account_url, container_name, blobpath, account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file\n";

    static constexpr std::string getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }
    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    void fromNamedCollectionImpl(const NamedCollection & collection, ContextPtr context);
    void fromDiskImpl(DiskPtr disk, ASTs & args, ContextPtr context, bool with_structure);
    void fromASTImpl(ASTs & args, ContextPtr context, bool with_structure, size_t max_number_of_arguments);

    Path blob_path;
    AzureBlobStorage::ConnectionParams connection_params;
};

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

    StorageAzureConfiguration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }

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

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;
    void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure) override;
    static bool collectCredentials(
        ASTPtr maybe_credentials, std::optional<String> & client_id, std::optional<String> & tenant_id, ContextPtr local_context);

    Path blob_path;
    Paths blobs_paths;
    AzureBlobStorage::ConnectionParams connection_params;
    DiskPtr disk;

private:
    void initializeFromParsableArguments(AzureStorageParsableArguments && parsable_arguments)
    {
        format = std::move(parsable_arguments.format);
        compression_method = std::move(parsable_arguments.compression_method);
        structure = std::move(parsable_arguments.structure);
        partition_strategy_type = parsable_arguments.partition_strategy_type;
        partition_columns_in_data_file = parsable_arguments.partition_columns_in_data_file;
        partition_strategy = std::move(parsable_arguments.partition_strategy);
        blob_path = parsable_arguments.blob_path;
        connection_params = std::move(parsable_arguments.connection_params);
    }
};
}

#endif
