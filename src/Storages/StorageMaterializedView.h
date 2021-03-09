#pragma once

#include <ext/shared_ptr_helper.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class StorageMaterializedView final : public ext::shared_ptr_helper<StorageMaterializedView>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMaterializedView>;
public:
    std::string getName() const override { return "MaterializedView"; }
    bool isView() const override { return true; }

    bool hasInnerTable() const { return has_inner_table; }

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context, const StorageMetadataPtr & /* metadata_snapshot */) const override
    {
        auto target_table = getTargetTable();
        auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
        return target_table->mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    void drop() override;
    void dropInnerTable(bool no_delay);

    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Context & context) override;

    void alter(const AlterCommands & params, const Context & context, TableLockHolder & table_lock_holder) override;

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & settings) const override;

    Pipe alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, const Context & context) override;

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void shutdown() override;

    void checkTableCanBeDropped() const override;
    void checkPartitionCanBeDropped(const ASTPtr & partition) override;

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, SelectQueryInfo &) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

    ActionLock getActionLock(StorageActionBlockType type) override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Strings getDataPaths() const override;

private:
    /// Will be initialized in constructor
    StorageID target_table_id = StorageID::createEmpty();

    Context & global_context;
    bool has_inner_table = false;

    void checkStatementCanBeForwarded() const;

protected:
    StorageMaterializedView(
        const StorageID & table_id_,
        Context & local_context,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        bool attach_);
};

}
