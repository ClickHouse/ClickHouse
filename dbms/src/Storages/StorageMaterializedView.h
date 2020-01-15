#pragma once

#include <ext/shared_ptr_helper.h>

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>


namespace DB
{

class StorageMaterializedView : public ext::shared_ptr_helper<StorageMaterializedView>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMaterializedView>;
public:
    std::string getName() const override { return "MaterializedView"; }

    ASTPtr getInnerQuery() const { return inner_query->clone(); }

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override
    {
        return getTargetTable()->mayBenefitFromIndexForIn(left_in_operand, query_context);
    }

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void drop(TableStructureWriteLockHolder &) override;

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void alterPartition(const ASTPtr & query, const PartitionCommands & commands, const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    void renameInMemory(const String & new_database_name, const String & new_table_name) override;

    void shutdown() override;

    void checkTableCanBeDropped() const override;
    void checkPartitionCanBeDropped(const ASTPtr & partition) override;

    QueryProcessingStage::Enum getQueryProcessingStage(const Context & context) const override;

    StoragePtr getTargetTable() const;
    StoragePtr tryGetTargetTable() const;

    ActionLock getActionLock(StorageActionBlockType type) override;

    BlockInputStreams read(
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
