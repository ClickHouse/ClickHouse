#pragma once
#include <Core/SchemaInferenceMode.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/IStorage.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>

#include <memory>

namespace DB
{

class ReadBufferIterator;
class SchemaCache;
struct StorageObjectStorageSettings;
using StorageObjectStorageSettingsPtr = std::shared_ptr<StorageObjectStorageSettings>;

/**
 * A general class containing implementation for external table engines
 * such as StorageS3, StorageAzure, StorageHDFS.
 * Works with an object of IObjectStorage class.
 */
class StorageObjectStorage : public IStorage
{
public:
    using ObjectInfo = RelativePathWithMetadata;
    using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;
    using ObjectInfos = std::vector<ObjectInfoPtr>;

    StorageObjectStorage(
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        LoadingStrictnessLevel mode,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr,
        bool is_table_function_ = false,
        bool lazy_init = false);

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

    bool supportsDynamicSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    static SchemaCache & getSchemaCache(const ContextPtr & context, const std::string & storage_type_name);

    static ColumnsDescription resolveSchemaFromData(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        std::string & sample_path,
        const ContextPtr & context);

    static std::string resolveFormatFromData(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        std::string & sample_path,
        const ContextPtr & context);

    static std::pair<ColumnsDescription, std::string> resolveSchemaAndFormatFromData(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        std::string & sample_path,
        const ContextPtr & context);

    void addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const override;

    bool updateExternalDynamicMetadataIfExists(ContextPtr query_context) override;

    IDataLakeMetadata * getExternalMetadata(ContextPtr query_context);

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

protected:
    /// Get path sample for hive partitioning implementation.
    String getPathSample(ContextPtr context);

    /// Creates ReadBufferIterator for schema inference implementation.
    static std::unique_ptr<ReadBufferIterator> createReadBufferIterator(
        const ObjectStoragePtr & object_storage,
        const StorageObjectStorageConfigurationPtr & configuration,
        const std::optional<FormatSettings> & format_settings,
        ObjectInfos & read_keys,
        const ContextPtr & context);

    /// Storage configuration (S3, Azure, HDFS, Local, DataLake).
    /// Contains information about table engine configuration
    /// and underlying storage access.
    StorageObjectStorageConfigurationPtr configuration;
    /// `object_storage` to allow direct access to data storage.
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    /// Partition by expression from CREATE query.
    const ASTPtr partition_by;
    /// Whether this engine is a part of according Cluster engine implementation.
    /// (One of the reading replicas, not the initiator).
    const bool distributed_processing;
    /// Whether we need to call `configuration->update()`
    /// (e.g. refresh configuration) on each read() method call.
    bool update_configuration_on_read_write = true;

    LoggerPtr log;
};

}
