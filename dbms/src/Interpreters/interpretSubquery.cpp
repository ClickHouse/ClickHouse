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
    const ASTSubquery * subquery = typeid_cast<const ASTSubquery *>(table_expression.get());
    const ASTFunction * function = typeid_cast<const ASTFunction *>(table_expression.get());
    const ASTIdentifier * table = typeid_cast<const ASTIdentifier *>(table_expression.get());

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

    ASTPtr query;
    if (table || function)
    {
        /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
        const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        query = select_with_union_query;

        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

        const auto select_query = std::make_shared<ASTSelectQuery>();
        select_with_union_query->list_of_selects->children.push_back(select_query);

        const auto select_expression_list = std::make_shared<ASTExpressionList>();
        select_query->select_expression_list = select_expression_list;
        select_query->children.emplace_back(select_query->select_expression_list);

        NamesAndTypesList columns;

        /// get columns list for target table
        if (function)
        {
            auto query_context = const_cast<Context *>(&context.getQueryContext());
            const auto & storage = query_context->executeTableFunction(table_expression);
            columns = storage->getColumns().ordinary;
            select_query->addTableFunction(*const_cast<ASTPtr *>(&table_expression));
        }
        else
        {
            DatabaseAndTableWithAlias database_table(*table);
            const auto & storage = context.getTable(database_table.database, database_table.table);
            columns = storage->getColumns().ordinary;
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

        /** Columns with the same name can be specified in a subquery. For example, SELECT x, x FROM t
          * This is bad, because the result of such a query can not be saved to the table, because the table can not have the same name columns.
          * Saving to the table is required for GLOBAL subqueries.
          *
          * To avoid this situation, we will rename the same columns.
          */

        std::set<std::string> all_column_names;
        std::set<std::string> assigned_column_names;

        if (ASTSelectWithUnionQuery * select_with_union = typeid_cast<ASTSelectWithUnionQuery *>(query.get()))
        {
            if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(select_with_union->list_of_selects->children.at(0).get()))
            {
                for (auto & expr : select->select_expression_list->children)
                    all_column_names.insert(expr->getAliasOrColumnName());

                for (auto & expr : select->select_expression_list->children)
                {
                    auto name = expr->getAliasOrColumnName();

                    if (!assigned_column_names.insert(name).second)
                    {
                        size_t i = 1;
                        while (all_column_names.end() != all_column_names.find(name + "_" + toString(i)))
                            ++i;

                        name = name + "_" + toString(i);
                        expr = expr->clone();   /// Cancels fuse of the same expressions in the tree.
                        expr->setAlias(name);

                        all_column_names.insert(name);
                        assigned_column_names.insert(name);
                    }
                }
            }
        }
    }

    return std::make_shared<InterpreterSelectWithUnionQuery>(
        query, subquery_context, required_source_columns, QueryProcessingStage::Complete, subquery_depth + 1);
}

}
