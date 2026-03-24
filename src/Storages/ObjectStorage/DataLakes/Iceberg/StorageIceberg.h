#pragma once

#include <Storages/ObjectStorage/DataLakes/StorageDataLake.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>

namespace DB
{

/**
 * Full explicit specialization of `StorageDataLake` for Iceberg.
 * Provides the same public interface as the generic template, plus
 * Iceberg-specific helpers (e.g. `getIcebergMetadata`).
 */
template <>
class StorageDataLake<IcebergMetadata> : public IStorage
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

    /// Returns the `IcebergMetadata` pointer, initializing it if needed.
    IcebergMetadata * getIcebergMetadata(ContextPtr context);

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

    bool optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Names & /* deduplicate_by_columns */,
        bool /*cleanup*/,
        ContextPtr context) override;

    bool supportsDelete() const override { return true; }

    bool supportsParallelInsert() const override { return true; }

    void mutate(const MutationCommands &, ContextPtr) override;
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const override;

    Pipe executeCommand(const String & command_name, const ASTPtr & args, ContextPtr context) override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

protected:
    StorageObjectStorageConfigurationPtr configuration;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const bool distributed_processing;
    bool supports_prewhere = false;
    bool supports_tuple_elements = false;
    bool is_table_function = false;

    LoggerPtr log;

    DataLakeStorageSettingsPtr datalake_settings;
    mutable std::shared_ptr<IcebergMetadata> current_metadata;

    void ensureMetadataInitialized(ContextPtr context) const;
    void updateMetadata(ContextPtr context) const;

    std::shared_ptr<DataLake::ICatalog> catalog;
    StorageID storage_id;
};

using StorageIceberg = StorageDataLake<IcebergMetadata>;

}
