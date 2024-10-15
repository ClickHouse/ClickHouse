#pragma once

#include <Parsers/IAST_fwd.h>

#include <Common/CurrentThread.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MaterializedView/RefreshTask.h>

namespace DB
{

class StorageMaterializedView final : public IStorage, WithMutableContext
{
public:
    StorageMaterializedView(
        const StorageID & table_id_,
        ContextPtr local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        LoadingStrictnessLevel mode,
        const String & comment);

    std::string getName() const override { return "MaterializedView"; }
    bool isView() const override { return true; }
    bool isRemote() const override;

    bool hasInnerTable() const { return has_inner_table; }

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
    bool supportsDynamicSubcolumns() const override { return getTargetTable()->supportsDynamicSubcolumns(); }
    bool supportsTransactions() const override { return getTargetTable()->supportsTransactions(); }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    Pipe alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr context) override;

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings, ContextPtr local_context) const override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void startup() override;
    void shutdown(bool is_drop) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;
    StorageID getTargetTableId() const;

    ActionLock getActionLock(StorageActionBlockType type) override;
    void onActionLockRemove(StorageActionBlockType action_type) override;

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    Strings getDataPaths() const override;

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    bool supportsBackupPartition() const override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;
    std::optional<UInt64> totalBytesUncompressed(const Settings & settings) const override;

private:
    mutable std::mutex target_table_id_mutex;
    /// Will be initialized in constructor
    StorageID target_table_id = StorageID::createEmpty();

    OwnedRefreshTask refresher;
    bool refresh_coordinated = false;

    bool has_inner_table = false;

    /// If false, inner table is replaced on each refresh. In that case, target_table_id doesn't
    /// have UUID, and we do inner table lookup by name instead.
    bool fixed_uuid = true;

    friend class RefreshTask;

    void checkStatementCanBeForwarded() const;

    ContextMutablePtr createRefreshContext() const;
    /// Prepare to refresh a refreshable materialized view: create temporary table (if needed) and
    /// form the insert-select query.
    /// out_temp_table_id may be assigned before throwing an exception, in which case the caller
    /// must drop the temp table before rethrowing.
    std::tuple<std::shared_ptr<ASTInsertQuery>, std::unique_ptr<CurrentThread::QueryScope>>
    prepareRefresh(bool append, ContextMutablePtr refresh_context, std::optional<StorageID> & out_temp_table_id) const;
    std::optional<StorageID> exchangeTargetTable(StorageID fresh_table, ContextPtr refresh_context) const;
    void dropTempTable(StorageID table, ContextMutablePtr refresh_context);

    void updateTargetTableId(std::optional<String> database_name, std::optional<String> table_name);
};

}
