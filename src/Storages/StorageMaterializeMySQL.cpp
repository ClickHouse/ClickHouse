#include <Storages/StorageMaterializeMySQL.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Processors/Pipe.h>

namespace DB
{

StorageMaterializeMySQL::StorageMaterializeMySQL(const StoragePtr & nested_storage_)
    : IStorage(nested_storage_->getStorageID()), nested_storage(nested_storage_)
{
    ColumnsDescription columns_desc;
    const ColumnsDescription & nested_columns_desc = nested_storage->getColumns();

    size_t index = 0;
    auto iterator = nested_columns_desc.begin();
    for (; index < nested_columns_desc.size() - 2; ++index, ++iterator)
        columns_desc.add(*iterator);

    setColumns(columns_desc);
}

Pipes StorageMaterializeMySQL::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams)
{
    if (ASTSelectQuery * select_query = query_info.query->as<ASTSelectQuery>())
    {
        auto & tables_in_select_query = select_query->tables()->as<ASTTablesInSelectQuery &>();

        if (!tables_in_select_query.children.empty())
        {
            auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();

            if (tables_element.table_expression)
                tables_element.table_expression->as<ASTTableExpression &>().final = true;
        }
    }

    Names require_columns_name = column_names;
    Block header = nested_storage->getSampleBlockNonMaterialized();
    ColumnWithTypeAndName & sign_column = header.getByPosition(header.columns() - 2);

    if (require_columns_name.end() == std::find(require_columns_name.begin(), require_columns_name.end(), sign_column.name))
        require_columns_name.emplace_back(sign_column.name);

    return nested_storage->read(require_columns_name, query_info, context, processed_stage, max_block_size, num_streams);
}

}
