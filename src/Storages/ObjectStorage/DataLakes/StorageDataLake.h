#pragma once
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/IStorage.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Databases/DataLake/ICatalog.h>

#include <memory>

#include <Storages/IPartitionStrategy.h>
namespace DB
{
struct IPartitionStrategy;

/**
 * A class containing implementation for data lake table engines
 * (Iceberg, DeltaLake, Hudi, Paimon).
 * Templated on the metadata type to allow compile-time specialization.
 * Works with an object of IObjectStorage class.
 */
template <typename DataLakeMetadata>
class StorageDataLake : public IStorage
{
public:
    StorageDataLake(
        StorageObjectStorageConfigurationPtr configuration_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_in_table_or_function_definition,
        const ConstraintsDescription & constraints_,
        const String & comment,
        std::optional<FormatSettings> format_settings_,
        LoadingStrictnessLevel mode,
        DataLakeStorageSettingsPtr datalake_settings_,
        std::shared_ptr<DataLake::ICatalog> catalog_,
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

    bool isDataLake() const override { return true; }

    bool isObjectStorage() const override { return true; }

    bool supportsReplication() const override { return true; }

    /// Things required for PREWHERE.
    bool supportsPrewhere() const override;
    bool canMoveConditionsToPrewhere() const override;
    std::optional<NameSet> supportedPrewhereColumns() const override;
    ColumnSizeByName getColumnSizes() const override;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    void addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const override;

    void updateExternalDynamicMetadataIfExists(ContextPtr query_context) override;

    IDataLakeMetadata * getExternalMetadata(ContextPtr query_context);

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

protected:
    /// Storage configuration (data lake specific).
    /// Contains information about table engine configuration
    /// and underlying storage access.
    StorageObjectStorageConfigurationPtr configuration;
    /// `object_storage` to allow direct access to data storage.
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    /// Whether this engine is a part of according Cluster engine implementation.
    /// (One of the reading replicas, not the initiator).
    const bool distributed_processing;
    bool supports_prewhere = false;
    bool supports_tuple_elements = false;
    bool is_table_function = false;

    LoggerPtr log;

    DataLakeStorageSettingsPtr datalake_settings;
    mutable DataLakeMetadataPtr current_metadata;

    void ensureMetadataInitialized(ContextPtr context) const;
    void updateMetadata(ContextPtr context) const;

    std::shared_ptr<DataLake::ICatalog> catalog;
    StorageID storage_id;
};

}
