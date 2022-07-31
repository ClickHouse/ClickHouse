#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

#include <Storages/IStorage.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth, const Names & required_source_columns)
{
    auto subquery_options = SelectQueryOptions(QueryProcessingStage::Complete, subquery_depth);
    return interpretSubquery(table_expression, context, required_source_columns, subquery_options);
}

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, const Names & required_source_columns, const SelectQueryOptions & options)
{
    if (auto * expr = table_expression->as<ASTTableExpression>())
    {
        ASTPtr table;
        if (expr->subquery)
            table = expr->subquery;
        else if (expr->table_function)
            table = expr->table_function;
        else if (expr->database_and_table_name)
            table = expr->database_and_table_name;

        return interpretSubquery(table, context, required_source_columns, options);
    }

    /// Subquery or table name. The name of the table is similar to the subquery `SELECT * FROM t`.
    const auto * subquery = table_expression->as<ASTSubquery>();
    const auto * function = table_expression->as<ASTFunction>();
    const auto * table = table_expression->as<ASTTableIdentifier>();

    if (!subquery && !table && !function)
        throw Exception("Table expression is undefined, Method: ExpressionAnalyzer::interpretSubquery." , ErrorCodes::LOGICAL_ERROR);

    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    auto subquery_context = Context::createCopy(context);
    Settings subquery_settings = context->getSettings();
    subquery_settings.max_result_rows = 0;
    subquery_settings.max_result_bytes = 0;
    /// The calculation of `extremes` does not make sense and is not necessary (if you do it, then the `extremes` of the subquery can be taken instead of the whole query).
    subquery_settings.extremes = false;
    subquery_context->setSettings(subquery_settings);

    auto subquery_options = options.subquery();

    ASTPtr query;
    if (table || function)
    {
        /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
        const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        query = select_with_union_query;

        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

        const auto select_query = std::make_shared<ASTSelectQuery>();
        select_with_union_query->list_of_selects->children.push_back(select_query);

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        const auto select_expression_list = select_query->select();

        NamesAndTypesList columns;
        /// get columns list for target table
        if (function)
        {
            auto query_context = context->getQueryContext();
            const auto & storage = query_context->executeTableFunction(table_expression);
            columns = storage->getInMemoryMetadataPtr()->getColumns().getOrdinary();
            select_query->addTableFunction(*const_cast<ASTPtr *>(&table_expression)); // XXX: const_cast should be avoided!
        }
        else
        {
            auto table_id = context->resolveStorageID(table_expression);
            const auto & storage = DatabaseCatalog::instance().getTable(table_id, context);
            columns = storage->getInMemoryMetadataPtr()->getColumns().getOrdinary();
            select_query->replaceDatabaseAndTable(table_id);
        }

        /// manually substitute column names in place of asterisk
        for (const auto & column : columns)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
    }
    else
    {
        query = subquery->children.front();
        subquery_options.removeDuplicates();
    }

    return std::make_shared<InterpreterSelectWithUnionQuery>(query, subquery_context, subquery_options, required_source_columns);
}

}
