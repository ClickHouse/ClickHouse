#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Storages/IStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

namespace DB
{

struct AzureSimpleAccountConfiguration
{
    std::string storage_account_url;
};

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using AzureConnectionString = std::string;

using AzureCredentials = std::variant<AzureSimpleAccountConfiguration, AzureConnectionString>;

class StorageAzure : public IStorage
{
public:
    using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
    using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

    struct Configuration : public StatelessTableEngineConfiguration
    {
        Configuration() = default;

        String getPath() const { return blob_path; }

        bool update(ContextPtr context);

        void connect(ContextPtr context);

        bool withGlobs() const { return blob_path.find_first_of("*?{") != std::string::npos; }

        bool withWildcard() const
        {
            static const String PARTITION_ID_WILDCARD = "{_partition_id}";
            return blobs_paths.back().find(PARTITION_ID_WILDCARD) != String::npos;
        }

        std::string getConnectionURL() const
        {
            if (!is_connection_string)
                return connection_url;

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Connection string not implemented yet");
        }

        std::string connection_url;
        bool is_connection_string;

        std::optional<std::string> account_name;
        std::optional<std::string> account_key;

        std::string container;
        std::string blob_path;
        std::vector<String> blobs_paths;
    };

    StorageAzure(
        const Configuration & configuration_,
        std::unique_ptr<AzureObjectStorage> && object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        ASTPtr partition_by_);

    static StorageAzure::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file = true);
    static AzureClientPtr createClient(StorageAzure::Configuration configuration);

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names &,
        const StorageSnapshotPtr &,
        SelectQueryInfo &,
        ContextPtr,
        QueryProcessingStage::Enum,
        size_t,
        size_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Read not implemented");
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr context) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

private:
    std::string name;
    Configuration configuration;
    std::unique_ptr<AzureObjectStorage> object_storage;
    NamesAndTypesList virtual_columns;
    Block virtual_block;

    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;

};

}

#endif
