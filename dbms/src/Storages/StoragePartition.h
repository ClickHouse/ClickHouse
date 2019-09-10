#pragma once

#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Core/Row.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IStorage.h>


namespace DB
{
class StoragePartition : public ext::shared_ptr_helper<StoragePartition>, public IStorage
{
    friend class PartitionBlockOutputStream;
    friend struct ext::shared_ptr_helper<StoragePartition>;

public:
    String getName() const override { return "Partition"; }
    String getTableName() const override { return table_name; }
    String getDatabaseName() const override { return database_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void drop(TableStructureWriteLockHolder &) override;

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    void rename(
        const String & /*new_path_to_db*/,
        const String & new_database_name,
        const String & new_table_name,
        TableStructureWriteLockHolder &) override
    {
        table_name = new_table_name;
        database_name = new_database_name;
    }

private:
    void initPartitionKey();
    void setProperties(const ASTPtr & new_order_by_ast, const ColumnsDescription & new_columns);

    String database_name;
    String table_name;

    Context global_context;

    ASTPtr partition_by_ast;
    ASTPtr order_by_ast;

    ExpressionActionsPtr partition_key_expr;
    Block partition_key_sample;

    Names sorting_key_columns;
    ASTPtr sorting_key_expr_ast;
    ExpressionActionsPtr sorting_key_expr;

    std::mutex mutex;
    Blocks data;

protected:
    StoragePartition(
        String database_name_,
        String table_name_,
        ColumnsDescription columns_,
        Context & context_,
        const ASTPtr & partition_by_ast_,
        const ASTPtr & order_by_ast_);
};

}
