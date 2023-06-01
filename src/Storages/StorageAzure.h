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
            return keys.back().find(PARTITION_ID_WILDCARD) != String::npos;
        }

        std::string connection_url;
        bool is_connection_string;

        std::optional<std::string> account_name;
        std::optional<std::string> account_key;

        std::string container;
        std::string blob_path;
        std::vector<String> blobs_paths;

        /// If s3 configuration was passed from ast, then it is static.
        /// If from config - it can be changed with config reload.
        bool static_configuration = true;

    };

    static StorageAzure::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file = true);

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr context) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);
};

}

#endif
