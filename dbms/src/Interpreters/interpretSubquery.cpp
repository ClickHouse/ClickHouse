#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

#include <Storages/IStorage.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, const Context & context, size_t subquery_depth, const Names & required_source_columns)
{
    /// Subquery or table name. The name of the table is similar to the subquery `SELECT * FROM t`.
    const auto * subquery = table_expression->as<ASTSubquery>();
    const auto * function = table_expression->as<ASTFunction>();
    const auto * table = table_expression->as<ASTIdentifier>();

    if (!subquery && !table && !function)
        throw Exception("Table expression is undefined, Method: ExpressionAnalyzer::interpretSubquery." , ErrorCodes::LOGICAL_ERROR);

    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    Context subquery_context = context;
    Settings subquery_settings = context.getSettings();
    subquery_settings.max_result_rows = 0;
    subquery_settings.max_result_bytes = 0;
    /// The calculation of `extremes` does not make sense and is not necessary (if you do it, then the `extremes` of the subquery can be taken instead of the whole query).
    subquery_settings.extremes = 0;
    subquery_context.setSettings(subquery_settings);

    auto subquery_options = SelectQueryOptions(QueryProcessingStage::Complete, subquery_depth).subquery();

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
            auto query_context = const_cast<Context *>(&context.getQueryContext());
            const auto & storage = query_context->executeTableFunction(table_expression);
            columns = storage->getColumns().getOrdinary();
            select_query->addTableFunction(*const_cast<ASTPtr *>(&table_expression)); // XXX: const_cast should be avoided!
        }
        else
        {
            DatabaseAndTableWithAlias database_table(*table);
            const auto & storage = context.getTable(database_table.database, database_table.table);
            columns = storage->getColumns().getOrdinary();
            select_query->replaceDatabaseAndTable(database_table.database, database_table.table);
        }

        select_expression_list->children.reserve(columns.size());
        /// manually substitute column names in place of asterisk
        for (const auto & column : columns)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
    }
    else
    {
        query = subquery->children.at(0);
        subquery_options.removeDuplicates();
    }

    return std::make_shared<InterpreterSelectWithUnionQuery>(query, subquery_context, subquery_options, required_source_columns);
}

}
