#pragma once
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/IStorage.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>

#include <memory>

#include <Storages/IPartitionStrategy.h>
namespace DB
{
class ReadBufferIterator;
class SchemaCache;
struct StorageObjectStorageSettings;
using StorageObjectStorageSettingsPtr = std::shared_ptr<StorageObjectStorageSettings>;
struct IPartitionStrategy;

/**
 * A general class containing implementation for external table engines
 * such as StorageS3, StorageAzure, StorageHDFS.
 * Works with an object of IObjectStorage class.
 */
class StorageObjectStorage : public IStorage
{
public:
    StorageObjectStorage(
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_in_table_or_function_definition,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        LoadingStrictnessLevel mode,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr,
        ASTPtr order_by_ = nullptr,
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

    void drop() override;

    bool supportsPartitionBy() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsDynamicSubcolumns() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool isDataLake() const override { return false; }

    bool isObjectStorage() const override { return true; }

    bool supportsReplication() const override { return false; }

    /// Things required for PREWHERE.
    bool supportsPrewhere() const override;
    bool canMoveConditionsToPrewhere() const override;
    std::optional<NameSet> supportedPrewhereColumns() const override;
    ColumnSizeByName getColumnSizes() const override;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    void addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

protected:
    /// Get path sample for hive partitioning implementation.
    String getPathSample(ContextPtr context);

    /// Storage configuration (S3, Azure, HDFS, Local, DataLake).
    /// Contains information about table engine configuration
    /// and underlying storage access.
    StorageObjectStorageConfigurationPtr configuration;
    StorageObjectStorageTableOptions table_options;
    /// `object_storage` to allow direct access to data storage.
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    /// Whether this engine is a part of according Cluster engine implementation.
    /// (One of the reading replicas, not the initiator).
    const bool distributed_processing;
    bool supports_prewhere = false;
    bool supports_tuple_elements = false;
    bool is_table_function = false;

    NamesAndTypesList hive_partition_columns_to_read_from_file_path;
    NamesAndTypesList file_columns;

    LoggerPtr log;
};

}
