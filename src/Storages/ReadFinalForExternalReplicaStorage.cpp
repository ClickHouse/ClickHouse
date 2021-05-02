#include <Storages/ReadFinalForExternalReplicaStorage.h>

#if USE_MYSQL || USE_LIBPQXX

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Interpreters/Context.h>


namespace DB
{

Pipe readFinalFromNestedStorage(
    StoragePtr nested_storage,
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams)
{
    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());
    auto lock = nested_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
    const StorageMetadataPtr & nested_metadata = nested_storage->getInMemoryMetadataPtr();

    Block nested_header = nested_metadata->getSampleBlock();
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

    Pipe pipe = nested_storage->read(require_columns_name, nested_metadata, query_info, context, processed_stage, max_block_size, num_streams);
    pipe.addTableLock(lock);

    if (!expressions->children.empty() && !pipe.empty())
    {
        Block pipe_header = pipe.getHeader();
        auto syntax = TreeRewriter(context).analyze(expressions, pipe_header.getNamesAndTypesList());
        ExpressionActionsPtr expression_actions = ExpressionAnalyzer(expressions, syntax, context).getActions(true /* add_aliases */, false /* project_result */);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(header, expression_actions, filter_column_name, false);
        });
    }

    return pipe;
}
}

#endif
