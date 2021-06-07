#pragma once

#include <ext/shared_ptr_helper.h>

#include <Common/OptimizedRegularExpression.h>
#include <Storages/IStorage.h>


namespace DB
{

/** A table that represents the union of an arbitrary number of other tables.
  * All tables must have the same structure.
  */
class StorageMerge final : public ext::shared_ptr_helper<StorageMerge>, public IStorage, WithContext
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
    bool supportsSubcolumns() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(const AlterCommands & params, ContextPtr context, TableLockHolder & table_lock_holder) override;

    bool mayBenefitFromIndexForIn(
        const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot) const override;

private:
    std::optional<OptimizedRegularExpression> source_database_regexp;
    std::optional<OptimizedRegularExpression> source_table_regexp;
    std::optional<std::unordered_map<String, std::unordered_set<String>>> source_databases_and_tables;

    /// (Database, Table, Lock, TableName)
    using StorageWithLockAndName = std::tuple<String, StoragePtr, TableLockHolder, String>;
    using StorageListWithLocks = std::list<StorageWithLockAndName>;
    using DatabaseTablesIterators = std::vector<DatabaseTablesIteratorPtr>;

    StorageMerge::StorageListWithLocks getSelectedTables(
            ContextPtr query_context, const ASTPtr & query = nullptr, bool filter_by_virtual_column = false) const;

    template <typename F>
    StoragePtr getFirstTable(F && predicate) const;

    DatabaseTablesIterators getDatabaseIterators(ContextPtr context) const;

    NamesAndTypesList getVirtuals() const override;
    ColumnSizeByName getColumnSizes() const override;

protected:
    StorageMerge(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        const std::unordered_map<String, std::unordered_set<String>> & source_databases_and_tables_,
        ContextPtr context_);

    StorageMerge(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        const String & source_database_regexp_,
        const String & source_table_regexp_,
        ContextPtr context_);

    Pipe createSources(
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const QueryProcessingStage::Enum & processed_stage,
        UInt64 max_block_size,
        const Block & header,
        const StorageWithLockAndName & storage_with_lock,
        Names & real_column_names,
        ContextMutablePtr modified_context,
        size_t streams_num,
        bool has_database_virtual_column,
        bool has_table_virtual_column,
        bool concat_streams = false);

    void convertingSourceStream(
        const Block & header, const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context, ASTPtr & query,
        Pipe & pipe, QueryProcessingStage::Enum processed_stage);
};

}
