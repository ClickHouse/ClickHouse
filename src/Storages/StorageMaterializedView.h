#pragma once

#include <Parsers/IAST_fwd.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Storages/MaterializedView/RefreshTask_fwd.h>

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

    bool hasInnerTable() const { return has_inner_table; }

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
    bool supportsTransactions() const override { return getTargetTable()->supportsTransactions(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        auto target_table = getTargetTable();
        auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
        return target_table->mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

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
        ContextPtr context) override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    Pipe alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr context) override;

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void startup() override;
    void shutdown() override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

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

private:
    mutable std::mutex target_table_id_mutex;
    /// Will be initialized in constructor
    StorageID target_table_id = StorageID::createEmpty();

    RefreshTaskHolder refresher;

    bool has_inner_table = false;

    friend class RefreshTask;

    void checkStatementCanBeForwarded() const;

    std::shared_ptr<ASTInsertQuery> prepareRefreshQuery();

    void updateInnerTableAfterRefresh(std::shared_ptr<ASTInsertQuery> refresh_query);

    StorageID getTargetTableId() const;
    void setTargetTableId(StorageID id);
    void updateTargetTableId(std::optional<String> database_name, std::optional<String> table_name);
};

}
