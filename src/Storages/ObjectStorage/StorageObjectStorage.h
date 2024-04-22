#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Storages/IStorage.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Processors/Formats/IInputFormat.h>

namespace DB
{

class StorageObjectStorageConfiguration;
class ReadBufferIterator;
class SchemaCache;

/**
 * A general class containing implementation for external table engines
 * such as StorageS3, StorageAzure, StorageHDFS.
 * Works with an object of IObjectStorage class.
 */
class StorageObjectStorage : public IStorage
{
public:
    using Configuration = StorageObjectStorageConfiguration;
    using ConfigurationPtr = std::shared_ptr<Configuration>;
    using ObjectInfo = RelativePathWithMetadata;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    struct QuerySettings
    {
        /// Insert settings:
        bool truncate_on_insert;
        bool create_new_file_on_insert;

        /// Schema inference settings:
        bool schema_inference_use_cache;
        SchemaInferenceMode schema_inference_mode;

        /// List settings:
        bool skip_empty_files;
        size_t list_object_keys_size;
        bool throw_on_zero_files_match;
        bool ignore_non_existent_file;
    };

    StorageObjectStorage(
        ConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr);

    String getName() const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder &) override;

    bool supportsPartitionBy() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    SchemaCache & getSchemaCache(const ContextPtr & context);

    static SchemaCache & getSchemaCache(const ContextPtr & context, const std::string & storage_type_name);

    static ColumnsDescription resolveSchemaFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    static std::string resolveFormatFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    static std::pair<ColumnsDescription, std::string> resolveSchemaAndFormatFromData(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

protected:
    virtual void updateConfiguration(ContextPtr local_context);

    static std::unique_ptr<ReadBufferIterator> createReadBufferIterator(
        const ObjectStoragePtr & object_storage,
        const ConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        ObjectInfos & read_keys,
        const ContextPtr & context);

    ConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    const std::string engine_name;
    const std::optional<FormatSettings> format_settings;
    const ASTPtr partition_by;
    const bool distributed_processing;

    LoggerPtr log;
    std::mutex configuration_update_mutex;
};

}
