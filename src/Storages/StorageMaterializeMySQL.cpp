#include <Storages/StorageMaterializeMySQL.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <Processors/Pipe.h>
#include <Processors/Transforms/FilterTransform.h>

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
    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());

    Block nested_header = nested_storage->getSampleBlockNonMaterialized();
    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);
    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);

    if (ASTSelectQuery * select_query = query_info.query->as<ASTSelectQuery>(); select_query && !column_names_set.count(version_column.name))
    {
        auto & tables_in_select_query = select_query->tables()->as<ASTTablesInSelectQuery &>();

        if (!tables_in_select_query.children.empty())
        {
            auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();

            if (tables_element.table_expression)
                tables_element.table_expression->as<ASTTableExpression &>().final = true;
        }
    }

    String filter_column_name;
    Names require_columns_name = column_names;
    ASTPtr expressions = std::make_shared<ASTExpressionList>();
    if (column_names_set.empty() || !column_names_set.count(sign_column.name))
    {
        require_columns_name.emplace_back(sign_column.name);

        const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));

        expressions->children.emplace_back(makeASTFunction("equals", sign_column_name, fetch_sign_value));
        filter_column_name = expressions->children.back()->getColumnName();

        for (const auto & column_name : column_names)
            expressions->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
    }

    Pipes pipes = nested_storage->read(require_columns_name, query_info, context, processed_stage, max_block_size, num_streams);

    if (!expressions->children.empty() && !pipes.empty())
    {
        Block pipe_header = pipes.front().getHeader();
        SyntaxAnalyzerResultPtr syntax = SyntaxAnalyzer(context).analyze(expressions, pipe_header.getNamesAndTypesList());
        ExpressionActionsPtr expression_actions = ExpressionAnalyzer(expressions, syntax, context).getActions(true);

        for (auto & pipe : pipes)
        {
            assertBlocksHaveEqualStructure(pipe_header, pipe.getHeader(), "StorageMaterializeMySQL");
            pipe.addSimpleTransform(std::make_shared<FilterTransform>(pipe.getHeader(), expression_actions, filter_column_name, false));
        }
    }

    return pipes;
}

NamesAndTypesList StorageMaterializeMySQL::getVirtuals() const
{
    NamesAndTypesList virtuals;
    Block nested_header = nested_storage->getSampleBlockNonMaterialized();
    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);
    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);
    virtuals.emplace_back(NameAndTypePair(sign_column.name, sign_column.type));
    virtuals.emplace_back(NameAndTypePair(version_column.name, version_column.type));

    auto nested_virtuals = nested_storage->getVirtuals();
    virtuals.insert(virtuals.end(), nested_virtuals.begin(), nested_virtuals.end());
    return virtuals;
}

}
