#pragma once

#include <Storages/IStorage.h>

namespace DB
{

/* An alias for another table. */
class StorageAlias final : public IStorage, WithMutableContext
{
public:
    StorageAlias(const StorageID & table_id_, const StorageID & ref_table_id_, ContextPtr context_);
    std::string getName() const override { return "Alias"; }

    bool supportsSampling() const override { return getReferenceTable()->supportsSampling(); }
    bool supportsFinal() const override { return getReferenceTable()->supportsFinal(); }
    bool supportsPartitionBy() const override { return getReferenceTable()->supportsPartitionBy(); }
    bool supportsTTL() const override { return getReferenceTable()->supportsTTL(); }
    bool supportsPrewhere() const override { return getReferenceTable()->supportsPrewhere(); }
    std::optional<NameSet> supportedPrewhereColumns() const override { return getReferenceTable()->supportedPrewhereColumns(); }
    bool canMoveConditionsToPrewhere() const override { return getReferenceTable()->canMoveConditionsToPrewhere(); }
    bool supportsReplication() const override { return getReferenceTable()->supportsReplication(); }
    bool supportsParallelInsert() const override { return getReferenceTable()->supportsParallelInsert(); }
    bool supportsDeduplication() const override { return getReferenceTable()->supportsDeduplication(); }
    bool noPushingToViewsOnInserts() const override { return getReferenceTable()->noPushingToViewsOnInserts(); }
    bool hasEvenlyDistributedRead() const override { return getReferenceTable()->hasEvenlyDistributedRead(); }
    bool supportsSubcolumns() const override { return getReferenceTable()->supportsSubcolumns(); }
    bool supportsOptimizationToSubcolumns() const override { return getReferenceTable()->supportsOptimizationToSubcolumns(); }
    bool supportsTransactions() const override { return getReferenceTable()->supportsTransactions(); }
    bool supportsDynamicSubcolumnsDeprecated() const override { return getReferenceTable()->supportsDynamicSubcolumnsDeprecated(); }
    bool supportsDynamicSubcolumns() const override { return getReferenceTable()->supportsDynamicSubcolumns(); }
    bool prefersLargeBlocks() const override { return getReferenceTable()->prefersLargeBlocks(); }
    bool areAsynchronousInsertsEnabled() const override { return getReferenceTable()->areAsynchronousInsertsEnabled(); }
    bool isSharedStorage() const override { return getReferenceTable()->isSharedStorage(); }
    ColumnSizeByName getColumnSizes() const override { return getReferenceTable()->getColumnSizes(); }
    IndexSizeByName getSecondaryIndexSizes() const override { return getReferenceTable()->getSecondaryIndexSizes(); }
    bool hasLightweightDeletedMask() const override { return getReferenceTable()->hasLightweightDeletedMask(); }
    bool supportsLightweightDelete() const override { return getReferenceTable()->supportsLightweightDelete(); }
    std::expected<void, PreformattedMessage> supportsLightweightUpdate() const override { return getReferenceTable()->supportsLightweightUpdate(); }
    bool hasProjection() const override { return getReferenceTable()->hasProjection(); }
    bool supportsDelete() const override { return getReferenceTable()->supportsDelete(); }
    bool supportsSparseSerialization() const override { return getReferenceTable()->supportsSparseSerialization(); }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const override { return getReferenceTable()->supportsTrivialCountOptimization(storage_snapshot, query_context); }

    ConditionSelectivityEstimatorPtr getConditionSelectivityEstimatorByPredicate(const StorageSnapshotPtr & snapshot, const ActionsDAG * dag, ContextPtr context_) const override
    {
        return getReferenceTable()->getConditionSelectivityEstimatorByPredicate(snapshot, dag, context_);
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context_,
        bool async_insert) override;

    std::optional<QueryPipeline> distributedWrite(const ASTInsertQuery & query, ContextPtr context_) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context_,
        TableExclusiveLockHolder & lock_holder) override;

    void alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & lock_holder) override;

    void updateExternalDynamicMetadataIfExists(ContextPtr context_) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context_) const override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr context_) override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr context_) const override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context_) override;

    QueryPipeline updateLightweight(const MutationCommands & commands, ContextPtr context_) override;

    void mutate(const MutationCommands & commands, ContextPtr context_) override;

    CancellationCode killMutation(const String & mutation_id) override;

    void waitForMutation(const String & mutation_id, bool wait_for_another_mutation) override;

    void setMutationCSN(const String & mutation_id, UInt64 csn) override;

    CancellationCode killPartMoveToShard(const UUID & task_uuid) override;

    StorageInMemoryMetadata getInMemoryMetadata() const override;
    StorageMetadataPtr getInMemoryMetadataPtr() const override;
    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;
    StorageSnapshotPtr getStorageSnapshotForQuery(const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query, ContextPtr query_context) const override;
    StorageSnapshotPtr getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

private:
    StoragePtr getReferenceTable() const;
    StorageID ref_table_id;
};

}
