#pragma once

#include <common/shared_ptr_helper.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class StorageMaterializedView final : public shared_ptr_helper<StorageMaterializedView>, public IStorage, WithMutableContext
{
    friend struct shared_ptr_helper<StorageMaterializedView>;
public:
    std::string getName() const override { return "MaterializedView"; }
    bool isView() const override { return true; }

    bool hasInnerTable() const { return has_inner_table; }

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool supportsSubcolumns() const override { return getTargetTable()->supportsSubcolumns(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        auto target_table = getTargetTable();
        auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
        return target_table->mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void drop() override;
    void dropInnerTableIfAny(bool no_delay, ContextPtr local_context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        ContextPtr context) override;

    void alter(const AlterCommands & params, ContextPtr context, TableLockHolder & table_lock_holder) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    Pipe alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr context) override;

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void shutdown() override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

    ActionLock getActionLock(StorageActionBlockType type) override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Strings getDataPaths() const override;

private:
    /// Will be initialized in constructor
    StorageID target_table_id = StorageID::createEmpty();

    bool has_inner_table = false;

    void checkStatementCanBeForwarded() const;

protected:
    StorageMaterializedView(
        const StorageID & table_id_,
        ContextPtr local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);
};

}
