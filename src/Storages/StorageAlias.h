#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Access/Common/AccessType.h>
#include <optional>

namespace DB
{

class StorageAlias final : public IStorage, WithContext
{
public:
    struct TargetAccess
    {
        ContextPtr context;
        AccessType access_type;
        Names column_names = {};
    };

    StorageAlias(
        const StorageID & table_id_,
        ContextPtr context_,
        const String & target_database_,
        const String & target_table_);

    std::string getName() const override { return "Alias"; }

    /// Get the target storage this alias points to
    StoragePtr getTargetTable(std::optional<TargetAccess> access_check = std::nullopt) const;

    /// Read from target table
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    /// Write to target table
    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        bool async_insert) override;

    /// Distributed write to target table
    std::optional<QueryPipeline> distributedWrite(const ASTInsertQuery & query, ContextPtr local_context) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override { getTargetTable()->checkAlterIsPossible(commands, local_context); }

    /// Alter target table
    void alter(
        const AlterCommands & params,
        ContextPtr local_context,
        AlterLockHolder & table_lock_holder) override;

    /// Truncate target table
    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder & table_lock_holder) override;

    /// Optimize target table
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr local_context) override;

    /// Alter partition on target table
    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr local_context) override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr local_context) const override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override { getTargetTable()->checkMutationIsPossible(commands, settings); }

    /// Mutate target table
    void mutate(
        const MutationCommands & commands,
        ContextPtr local_context) override;

    /// Lightweight update on target table
    QueryPipeline updateLightweight(const MutationCommands & commands, ContextPtr local_context) override;

    CancellationCode killMutation(const String & mutation_id) override;
    void waitForMutation(const String & mutation_id, bool wait_for_another_mutation) override;
    void setMutationCSN(const String & mutation_id, UInt64 csn) override;

    void updateExternalDynamicMetadataIfExists(ContextPtr local_context) override;
    void checkTableCanBeDropped(ContextPtr /*query_context*/) const override {}
    StorageInMemoryMetadata getInMemoryMetadata() const override { return getTargetTable()->getInMemoryMetadata(); }
    StorageMetadataPtr getInMemoryMetadataPtr(bool bypass_metadata_cache) const override { return getTargetTable()->getInMemoryMetadataPtr(bypass_metadata_cache); }
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;
    StorageSnapshotPtr getStorageSnapshotForQuery(const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query, ContextPtr query_context) const override;
    StorageSnapshotPtr getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
    bool supportsDynamicSubcolumns() const override { return getTargetTable()->supportsDynamicSubcolumns(); }
    bool supportsDynamicSubcolumnsDeprecated() const override { return getTargetTable()->supportsDynamicSubcolumnsDeprecated(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    std::optional<NameSet> supportedPrewhereColumns() const override { return getTargetTable()->supportedPrewhereColumns(); }
    bool canMoveConditionsToPrewhere() const override { return getTargetTable()->canMoveConditionsToPrewhere(); }
    bool supportsOptimizationToSubcolumns() const override { return getTargetTable()->supportsOptimizationToSubcolumns(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool supportsDeduplication() const override { return getTargetTable()->supportsDeduplication(); }
    bool supportsTransactions() const override { return getTargetTable()->supportsTransactions(); }
    bool noPushingToViewsOnInserts() const override { return getTargetTable()->noPushingToViewsOnInserts(); }
    bool hasEvenlyDistributedRead() const override { return getTargetTable()->hasEvenlyDistributedRead(); }
    bool prefersLargeBlocks() const override { return getTargetTable()->prefersLargeBlocks(); }
    bool areAsynchronousInsertsEnabled() const override { return getTargetTable()->areAsynchronousInsertsEnabled(); }
    bool isRemote() const override { return getTargetTable()->isRemote(); }
    bool isSharedStorage() const override { return getTargetTable()->isSharedStorage(); }

    bool hasLightweightDeletedMask() const override { return getTargetTable()->hasLightweightDeletedMask(); }
    bool supportsLightweightDelete() const override { return getTargetTable()->supportsLightweightDelete(); }
    std::expected<void, PreformattedMessage> supportsLightweightUpdate() const override { return getTargetTable()->supportsLightweightUpdate(); }
    bool supportsDelete() const override { return getTargetTable()->supportsDelete(); }
    bool hasProjection() const override { return getTargetTable()->hasProjection(); }
    bool supportsSparseSerialization() const override { return getTargetTable()->supportsSparseSerialization(); }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const override { return getTargetTable()->supportsTrivialCountOptimization(storage_snapshot, query_context); }
    bool supportsPartitionBy() const override { return getTargetTable()->supportsPartitionBy(); }
    bool supportsTTL() const override { return getTargetTable()->supportsTTL(); }

    NamesAndTypesList getVirtuals() const { return getTargetTable()->getVirtualsList(); }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr local_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const override;

    Strings getDataPaths() const override { return getTargetTable()->getDataPaths(); }

    ActionLock getActionLock(StorageActionBlockType type) override { return getTargetTable()->getActionLock(type); }

    std::optional<UInt64> totalRows(ContextPtr query_context) const override { return getTargetTable()->totalRows(query_context); }
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override { return getTargetTable()->totalBytes(query_context); }
    ColumnSizeByName getColumnSizes() const override { return getTargetTable()->getColumnSizes(); }
    IndexSizeByName getSecondaryIndexSizes() const override { return getTargetTable()->getSecondaryIndexSizes(); }

    CancellationCode killPartMoveToShard(const UUID & task_uuid) override;

    /// These operations are not proxied (executed on alias itself)
    /// Drop alias, not the target table
    void drop() override {}

    /// Rename alias, not the target table
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

protected:
    String target_database;
    String target_table;
};

}
