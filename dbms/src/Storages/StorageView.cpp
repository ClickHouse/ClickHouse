#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/getTableExpressions.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>

#include <DataStreams/MaterializingBlockInputStream.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int ALIAS_REQUIRED;
}


StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : IStorage(table_id_)
{
    setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    inner_query = query.select->ptr();
}


BlockInputStreams StorageView::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    BlockInputStreams res;

    ASTPtr current_inner_query = inner_query;

    if (context.getSettings().enable_optimize_predicate_expression)
    {
        auto new_inner_query = inner_query->clone();
        auto new_outer_query = query_info.query->clone();
        auto * new_outer_select = new_outer_query->as<ASTSelectQuery>();

        replaceTableNameWithSubquery(new_outer_select, new_inner_query);

        /// TODO: remove getTableExpressions and getTablesWithColumns
        {
            const auto & table_expressions = getTableExpressions(*new_outer_select);
            const auto & tables_with_columns = getDatabaseAndTablesWithColumnNames(table_expressions, context);

            auto & settings = context.getSettingsRef();
            if (settings.joined_subquery_requires_alias && tables_with_columns.size() > 1)
            {
                for (auto & pr : tables_with_columns)
                    if (pr.table.table.empty() && pr.table.alias.empty())
                        throw Exception("Not unique subquery in FROM requires an alias (or joined_subquery_requires_alias=0 to disable restriction).",
                            ErrorCodes::ALIAS_REQUIRED);
            }

            if (PredicateExpressionsOptimizer(context, tables_with_columns, context.getSettings()).optimize(*new_outer_select))
                current_inner_query = new_inner_query;
        }
    }

    QueryPipeline pipeline;
    /// FIXME res may implicitly use some objects owned be pipeline, but them will be destructed after return
    res = InterpreterSelectWithUnionQuery(current_inner_query, context, {}, column_names).executeWithMultipleStreams(pipeline);

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    for (auto & stream : res)
        stream = std::make_shared<MaterializingBlockInputStream>(stream);

    return res;
}

void StorageView::replaceTableNameWithSubquery(ASTSelectQuery * select_query, ASTPtr & subquery)
{
    auto * select_element = select_query->tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    auto * table_expression = select_element->table_expression->as<ASTTableExpression>();

    if (!table_expression->database_and_table_name)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    const auto alias = table_expression->database_and_table_name->tryGetAlias();
    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->children.push_back(subquery);
    if (!alias.empty())
        table_expression->subquery->setAlias(alias);
}


void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return StorageView::create(args.table_id, args.query, args.columns);
    });
}

}
