#pragma once

#include <ext/shared_ptr_helper.h>

#include <Common/OptimizedRegularExpression.h>
#include <Storages/IStorage.h>


namespace DB
{

/** A table that represents the union of an arbitrary number of other tables.
  * All tables must have the same structure.
  */
class StorageMerge : public ext::shared_ptr_helper<StorageMerge>, public IStorage
{
public:
    std::string getName() const override { return "Merge"; }
    std::string getTableName() const override { return name; }

    bool isRemote() const override;

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &) const override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void drop() override {}
    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override { name = new_table_name; }

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(
        const AlterCommands & params, const String & database_name, const String & table_name,
        const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override;

private:
    String name;
    String source_database;
    OptimizedRegularExpression table_name_regexp;
    Context global_context;

    using StorageListWithLocks = std::list<std::pair<StoragePtr, TableStructureReadLockHolder>>;

    StorageListWithLocks getSelectedTables(const String & query_id) const;

    StorageMerge::StorageListWithLocks getSelectedTables(const ASTPtr & query, bool has_virtual_column, bool get_lock, const String & query_id) const;

    template <typename F>
    StoragePtr getFirstTable(F && predicate) const;

protected:
    StorageMerge(
        const std::string & name_,
        const ColumnsDescription & columns_,
        const String & source_database_,
        const String & table_name_regexp_,
        const Context & context_);

    Block getQueryHeader(const Names & column_names, const SelectQueryInfo & query_info,
                         const Context & context, QueryProcessingStage::Enum processed_stage);

    BlockInputStreams createSourceStreams(const SelectQueryInfo & query_info, const QueryProcessingStage::Enum & processed_stage,
                                          const UInt64 max_block_size, const Block & header, const StoragePtr & storage,
                                          const TableStructureReadLockHolder & struct_lock, Names & real_column_names,
                                          Context & modified_context, size_t streams_num, bool has_table_virtual_column,
                                          bool concat_streams = false);

    void convertingSourceStream(const Block & header, const Context & context, ASTPtr & query,
                                BlockInputStreamPtr & source_stream, QueryProcessingStage::Enum processed_stage);
};

}
