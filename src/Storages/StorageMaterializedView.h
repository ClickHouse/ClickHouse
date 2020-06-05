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

    ASTPtr getSelectQuery() const { return select->clone(); }
    ASTPtr getInnerQuery() const { return inner_query->clone(); }
    bool hasInnerTable() const { return has_inner_table; }

    StorageInMemoryMetadata getInMemoryMetadata() const override;

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool supportsParallelInsert() const override { return getTargetTable()->supportsParallelInsert(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override
    {
        return getTargetTable()->mayBenefitFromIndexForIn(left_in_operand, query_context);
    }

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void drop() override;

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void alter(const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & settings) override;

    void alterPartition(const ASTPtr & query, const PartitionCommands & commands, const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void shutdown() override;

    void checkTableCanBeDropped() const override;
    void checkPartitionCanBeDropped(const ASTPtr & partition) override;

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, const ASTPtr &) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

    ActionLock getActionLock(StorageActionBlockType type) override;

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Strings getDataPaths() const override;

private:
    /// Can be empty if SELECT query doesn't contain table
    StorageID select_table_id = StorageID::createEmpty();
    /// Will be initialized in constructor
    StorageID target_table_id = StorageID::createEmpty();

    ASTPtr select;
    ASTPtr inner_query;

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
