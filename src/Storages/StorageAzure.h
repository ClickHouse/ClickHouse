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

        Poco::URI getConnectionURL() const;

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
    static AzureObjectStorage::SettingsPtr createSettings(StorageAzure::Configuration configuration);
    static ColumnsDescription getTableStructureFromData(
        const StorageAzure::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

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

    bool supportsSubcolumns() const override;

    bool supportsSubsetOfColumns() const override;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

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

    static ColumnsDescription getTableStructureFromDataImpl(
        const Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

};

class StorageAzureSource : public ISource, WithContext
{
public:
    StorageAzureSource (std::unique_ptr<ReadBufferFromFileBase> && read_buffer_, ContextPtr context_, const Block & sample_block_, UInt64 max_block_size_, const ColumnsDescription & columns_);
    ~StorageAzureSource() override {}

    Chunk generate() override;
    String getName() const override;


private:
//    std::unique_ptr<ReadBufferFromFileBase> read_buffer;

    String path;
    std::unique_ptr<ReadBufferFromFileBase> read_buffer;
//    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    Block sample_block;
    UInt64 max_block_size;
    ColumnsDescription columns_desc;

//    void createReader();
>>>>>>> origin/azure_table_function
};

}

#endif
