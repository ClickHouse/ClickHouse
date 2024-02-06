#pragma once

#include <Parsers/IAST_fwd.h>

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
        bool attach_,
        const String & comment);

    std::string getName() const override { return "MaterializedView"; }
    bool isView() const override { return true; }
    bool isRemote() const override;

    std::vector<StorageID> innerTables() const;

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
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

    /// Get the virtual column of the target table;
    NamesAndTypesList getVirtuals() const override;

    ActionLock getActionLock(StorageActionBlockType type) override;
    void onActionLockRemove(StorageActionBlockType action_type) override;

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
    friend class RefreshTask;

    mutable std::mutex inner_table_ids_mutex;
    /// Inner table `.inner_id.<uuid>` owned by the MV, or non-owned table specified with TO.
    StorageID target_table_id = StorageID::createEmpty();
    /// Table `.inner_scratch_id.<uuid>` owned by us, if needed.
    StorageID scratch_table_id = StorageID::createEmpty();

    OwnedRefreshTask refresher;
    bool refresh_on_start = false;

    bool has_inner_target_table = false;
    bool has_scratch_table = false;

    bool scratch_table_is_known_to_be_empty = false;

    void checkStatementCanBeForwarded() const;

    StorageID getScratchTableId() const;
    StoragePtr tryGetScratchTable() const;
    StoragePtr getScratchTable() const;

    ContextMutablePtr createRefreshContext() const;
    /// Prepare an INSERT SELECT query for refreshing a refreshable materialized view.
    std::shared_ptr<ASTInsertQuery> prepareRefresh(ContextMutablePtr refresh_context);
    void transferRefreshedData(ContextPtr refresh_context);
};

}
