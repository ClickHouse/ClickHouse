#pragma once

#include <ext/shared_ptr_helper.h>

#include <Common/OptimizedRegularExpression.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>


namespace DB
{

/** A table that represents the union of an arbitrary number of other tables.
  * All tables must have the same structure.
  */
class StorageMerge final : public ext::shared_ptr_helper<StorageMerge>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMerge>;
public:
    std::string getName() const override { return "Merge"; }

    bool isRemote() const override;

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, const ASTPtr &) const override;

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) const override;

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(const AlterCommands & params, const Context & context, TableLockHolder & table_lock_holder) override;

    bool mayBenefitFromIndexForIn(
        const ASTPtr & left_in_operand, const Context & query_context, const StorageMetadataPtr & metadata_snapshot) const override;

private:
    String source_database;
    OptimizedRegularExpression table_name_regexp;
    Context global_context;

    using StorageWithLockAndName = std::tuple<StoragePtr, TableLockHolder, String>;
    using StorageListWithLocks = std::list<StorageWithLockAndName>;

    StorageListWithLocks getSelectedTables(const String & query_id, const Settings & settings) const;

    StorageMerge::StorageListWithLocks getSelectedTables(
            const ASTPtr & query, bool has_virtual_column, const String & query_id, const Settings & settings) const;

    template <typename F>
    StoragePtr getFirstTable(F && predicate) const;

    DatabaseTablesIteratorPtr getDatabaseIterator(const Context & context) const;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageMerge(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & source_database_,
        const String & table_name_regexp_,
        const Context & context_);

    Block getQueryHeader(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage);

    Pipes createSources(
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const QueryProcessingStage::Enum & processed_stage,
        const UInt64 max_block_size,
        const Block & header,
        const StorageWithLockAndName & storage_with_lock,
        Names & real_column_names,
        const std::shared_ptr<Context> & modified_context,
        size_t streams_num,
        bool has_table_virtual_column,
        bool concat_streams = false);

    void convertingSourceStream(
        const Block & header, const StorageMetadataPtr & metadata_snapshot,
        const Context & context, ASTPtr & query,
        Pipe & pipe, QueryProcessingStage::Enum processed_stage);
};

}
