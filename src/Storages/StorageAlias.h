#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

class StorageAlias final : public IStorage, WithContext
{
public:
    StorageAlias(
        const StorageID & table_id_,
        ContextPtr context_,
        const StorageID & target_table_id_,
        const ColumnsDescription & columns_,
        const String & comment);

    std::string getName() const override { return "Alias"; }

    /// Get the target storage this alias points to
    StoragePtr getTargetTable() const { return DatabaseCatalog::instance().getTable(target_table_id, getContext()); }
    StorageID getTargetTableId() const { return target_table_id; }

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

    void checkTableCanBeDropped(ContextPtr /*query_context*/) const override {}

    void checkTableCanBeRenamed(const StorageID & /*new_name*/) const override {}

    /// Proxy to target table
    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
    bool supportsDynamicSubcolumns() const override { return getTargetTable()->supportsDynamicSubcolumns(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsReplication() const override { return getTargetTable()->supportsReplication(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool supportsDeduplication() const override { return getTargetTable()->supportsDeduplication(); }
    bool supportsTransactions() const override { return getTargetTable()->supportsTransactions(); }
    bool isRemote() const override { return getTargetTable()->isRemote(); }

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

    /// These operations are not proxied (executed on alias itself)
    /// Drop alias, not the target table
    void drop() override {}

    /// Rename alias, not the target table
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;


protected:
    StorageID target_table_id = StorageID::createEmpty();
};

}
