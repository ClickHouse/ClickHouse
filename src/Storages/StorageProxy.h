#pragma once

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

class StorageProxy : public IStorage
{
public:

    explicit StorageProxy(const StorageID & table_id_) : IStorage(table_id_) {}

    virtual StoragePtr getNested() const = 0;

    /// The wrapped storage if it already exists, or null. Never creates it, so that observers
    /// iterating every table cannot trigger a lazy load or a remote call for a table function.
    virtual StoragePtr tryGetNested() const { return nullptr; }

    String getName() const override { return "Proxy"; }

    bool isRemote() const override { return getNested()->isRemote(); }
    bool isView() const override { return getNested()->isView(); }
    bool supportsTruncate() const override { return getNested()->supportsTruncate(); }
    bool supportsSampling() const override { return getNested()->supportsSampling(); }
    bool supportsFinal() const override { return getNested()->supportsFinal(); }
    bool supportsPrewhere() const override { return getNested()->supportsPrewhere(); }
    bool canMoveConditionsToPrewhere() const override { return getNested()->canMoveConditionsToPrewhere(); }
    std::optional<NameSet> supportedPrewhereColumns() const override { return getNested()->supportedPrewhereColumns(); }
    bool supportedPrewhereColumnsIncludeSubcolumns() const override { return getNested()->supportedPrewhereColumnsIncludeSubcolumns(); }
    bool supportsReplication() const override { return getNested()->supportsReplication(); }
    bool supportsParallelInsert() const override { return getNested()->supportsParallelInsert(); }
    bool supportsDeduplication() const override { return getNested()->supportsDeduplication(); }
    bool noPushingToViewsOnInserts() const override { return getNested()->noPushingToViewsOnInserts(); }
    bool hasEvenlyDistributedRead() const override { return getNested()->hasEvenlyDistributedRead(); }
    bool supportsSubcolumns() const override { return getNested()->supportsSubcolumns(); }
    /// The IStorage default ties this to supportsSubcolumns(); forward it so a proxy around a
    /// storage that opts out of the rewrite (e.g. Distributed) does not re-advertise true.
    bool supportsOptimizationToSubcolumns() const override { return getNested()->supportsOptimizationToSubcolumns(); }
    bool supportsColumnsWithDynamicStructure() const override { return getNested()->supportsColumnsWithDynamicStructure(); }

    ColumnSizeByName getColumnSizes() const override { return getNested()->getColumnSizes(); }
    ColumnSizeByName getColumnSizes(const Names & columns) const override { return getNested()->getColumnSizes(columns); }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & base_metadata, ContextPtr query_context) const override
    {
        auto nested_metadata = getNested()->getInMemoryMetadataPtr(query_context, false);
        auto new_metadata = std::make_shared<StorageInMemoryMetadata>(base_metadata->withVirtuals(nested_metadata->virtuals));
        return std::make_shared<StorageSnapshot>(*this, std::move(new_metadata));
    }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr &,
        SelectQueryInfo & info) const override
    {
        const auto nested_metadata = getNested()->getInMemoryMetadataPtr(context, false);
        return getNested()->getQueryProcessingStage(context, to_stage, getNested()->getStorageSnapshot(nested_metadata, context), info);
    }

    Pipe watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        return getNested()->watch(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        getNested()->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override
    {
        return getNested()->write(query, metadata_snapshot, context, async_insert);
    }

    void checkInsertIsAllowed(ContextPtr context) const override { getNested()->checkInsertIsAllowed(context); }

    void drop() override { getNested()->drop(); }

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & lock) override
    {
        getNested()->truncate(query, metadata_snapshot, context, lock);
    }

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override
    {
        getNested()->rename(new_path_to_table_data, new_table_id);
        IStorage::renameInMemory(new_table_id);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        getNested()->renameInMemory(new_table_id);
        IStorage::renameInMemory(new_table_id);
    }

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder) override
    {
        getNested()->alter(params, context, alter_lock_holder);
        auto nested_metadata = getNested()->getInMemoryMetadataPtr(context, true);
        IStorage::setInMemoryMetadata(*nested_metadata);
    }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override
    {
        getNested()->checkAlterIsPossible(commands, context);
    }

    Pipe alterPartition(
            const StorageMetadataPtr & metadata_snapshot,
            const PartitionCommands & commands,
            ContextPtr context) override
    {
        return getNested()->alterPartition(metadata_snapshot, commands, context);
    }

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings, ContextPtr context) const override
    {
        getNested()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings, context);
    }

    bool optimize(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const ASTPtr & partition,
            bool final,
            bool deduplicate,
            const Names & deduplicate_by_columns,
            bool cleanup,
            ContextPtr context) override
    {
        return getNested()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, cleanup, context);
    }

    void mutate(const MutationCommands & commands, ContextPtr context) override { getNested()->mutate(commands, context); }

    /// Without this the base implementation rejects every mutation before `mutate` is reached.
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override
    {
        getNested()->checkMutationIsPossible(commands, settings);
    }

    bool supportsDelete() const override { return getNested()->supportsDelete(); }
    bool supportsLightweightDelete() const override { return getNested()->supportsLightweightDelete(); }

    /// `UPDATE` and `DELETE FROM` ask the catalog pointer before running, so both the check and the
    /// execution have to reach the nested storage.
    std::expected<void, PreformattedMessage> supportsLightweightUpdate() const override
    {
        return getNested()->supportsLightweightUpdate();
    }

    QueryPipeline updateLightweight(const MutationCommands & commands, ContextPtr context) override
    {
        return getNested()->updateLightweight(commands, context);
    }

    /// Gates `ALTER TABLE ... MODIFY TTL`.
    bool supportsTTL() const override { return getNested()->supportsTTL(); }
    /// Gates `SELECT ... FROM t STREAM`.
    bool supportsStreaming() const override { return getNested()->supportsStreaming(); }
    bool supportsTransactions() const override { return getNested()->supportsTransactions(); }
    bool supportsSparseSerialization() const override { return getNested()->supportsSparseSerialization(); }

    CancellationCode killMutation(const String & mutation_id) override { return getNested()->killMutation(mutation_id); }

    /// `IStorage::backupData` is a no-op, so an unforwarded proxy would produce an empty backup
    /// that still reports success.
    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override
    {
        getNested()->backupData(backup_entries_collector, data_path_in_backup, partitions);
    }

    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override
    {
        getNested()->restoreDataFromBackup(restorer, data_path_in_backup, partitions);
    }

    bool supportsBackupPartition() const override { return getNested()->supportsBackupPartition(); }
    void finalizeRestoreFromBackup() override { getNested()->finalizeRestoreFromBackup(); }

    /// The planner uses this to decide parallel replica eligibility, and the base default of
    /// false silently disables parallel replicas for the nested table.
    bool isMergeTree() const override { return getNested()->isMergeTree(); }

    /// Gates the table-level `async_insert` setting, which is otherwise silently ignored.
    bool areAsynchronousInsertsEnabled() const override { return getNested()->areAsynchronousInsertsEnabled(); }

    /// The proxy's snapshot is bound to the proxy and carries no engine-specific data, which the
    /// nested storage would misread, so build a snapshot from the nested storage instead.
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr query_context) const override
    {
        auto nested = getNested();
        auto nested_metadata = nested->getInMemoryMetadataPtr(query_context, false);
        auto nested_snapshot = nested->getStorageSnapshot(nested_metadata, query_context);
        return nested->supportsTrivialCountOptimization(nested_snapshot, query_context);
    }

    void startup() override { getNested()->startup(); }
    void shutdown(bool is_drop) override { getNested()->shutdown(is_drop); }
    void flushAndPrepareForShutdown() override { getNested()->flushAndPrepareForShutdown(); }

    ActionLock getActionLock(StorageActionBlockType action_type) override { return getNested()->getActionLock(action_type); }

    DataValidationTasksPtr getCheckTaskList(const CheckTaskFilter & check_task_filter, ContextPtr context) override
    {
        return getNested()->getCheckTaskList(check_task_filter, context);
    }

    std::optional<CheckResult> checkDataNext(DataValidationTasksPtr & check_task_list) override
    {
        return getNested()->checkDataNext(check_task_list);
    }

    void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const override { getNested()->checkTableCanBeDropped(query_context); }
    void checkTableSizeBelowDropLimit([[ maybe_unused ]] ContextPtr query_context) const override { getNested()->checkTableSizeBelowDropLimit(query_context); }

    bool storesDataOnDisk() const override { return getNested()->storesDataOnDisk(); }
    Strings getDataPaths() const override { return getNested()->getDataPaths(); }
    StoragePolicyPtr getStoragePolicy() const override { return getNested()->getStoragePolicy(); }
    std::optional<UInt64> totalRows(ContextPtr query_context) const override { return getNested()->totalRows(query_context); }
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override { return getNested()->totalBytes(query_context); }
    std::optional<UInt64> lifetimeRows() const override { return getNested()->lifetimeRows(); }
    std::optional<UInt64> lifetimeBytes() const override { return getNested()->lifetimeBytes(); }

};

/// Resolves a proxy to the storage it wraps, for callers that reach a storage by casting to a
/// concrete engine type. A lazily loaded table is a proxy until its first access; while the real
/// storage does not exist yet the proxy is returned unchanged, so such a cast still fails and the
/// caller skips the table instead of forcing it to load.
inline StoragePtr resolveStorageProxy(const StoragePtr & storage)
{
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get()))
    {
        if (auto nested = proxy->tryGetNested())
            return nested;
    }
    return storage;
}

/// Same, but creates the wrapped storage when it does not exist yet. For operations that name a
/// table explicitly, where loading it is the expected cost of the operation.
inline StoragePtr resolveStorageProxyLoading(const StoragePtr & storage)
{
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get()))
        return proxy->getNested();
    return storage;
}

}
