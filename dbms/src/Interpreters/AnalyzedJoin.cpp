#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Storages/IStorage.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

ExpressionActionsPtr AnalyzedJoin::createJoinedBlockActions(
    const JoinedColumnsList & columns_added_by_join,
    const ASTSelectQuery * select_query_with_join,
    const Context & context) const
{
    if (!select_query_with_join)
        return nullptr;

    const ASTTablesInSelectQueryElement * join = select_query_with_join->join();

    if (!join)
        return nullptr;

    const auto & join_params = static_cast<const ASTTableJoin &>(*join->table_join);

    /// Create custom expression list with join keys from right table.
    auto expression_list = std::make_shared<ASTExpressionList>();
    ASTs & children = expression_list->children;

    if (join_params.on_expression)
        for (const auto & join_right_key : key_asts_right)
            children.emplace_back(join_right_key);

    NameSet required_columns_set(key_names_right.begin(), key_names_right.end());
    for (const auto & joined_column : columns_added_by_join)
        required_columns_set.insert(joined_column.name_and_type.name);
    Names required_columns(required_columns_set.begin(), required_columns_set.end());

    NamesAndTypesList source_column_names;
    for (auto & column : columns_from_joined_table)
        source_column_names.emplace_back(column.name_and_type);

    ASTPtr query = expression_list;
    auto syntax_result = SyntaxAnalyzer(context).analyze(query, source_column_names, required_columns);
    ExpressionAnalyzer analyzer(query, syntax_result, context, {}, required_columns_set);
    return analyzer.getActions(false);
}

Names AnalyzedJoin::getOriginalColumnNames(const NameSet & required_columns_from_joined_table) const
{
    Names original_columns;
    for (const auto & column : columns_from_joined_table)
        if (required_columns_from_joined_table.count(column.name_and_type.name))
            original_columns.emplace_back(column.original_name);
    return original_columns;
}

const JoinedColumnsList & AnalyzedJoin::getColumnsFromJoinedTable(
        const NameSet & source_columns, const Context & context, const ASTSelectQuery * select_query_with_join)
{
    if (select_query_with_join && columns_from_joined_table.empty())
    {
        if (const ASTTablesInSelectQueryElement * node = select_query_with_join->join())
        {
            const auto & table_expression = static_cast<const ASTTableExpression &>(*node->table_expression);
            DatabaseAndTableWithAlias table_name_with_alias(table_expression, context.getCurrentDatabase());

            auto columns = getNamesAndTypeListFromTableExpression(table_expression, context);

            for (auto & column : columns)
            {
                JoinedColumn joined_column(column, column.name);

                if (source_columns.count(column.name))
                {
                    auto qualified_name = table_name_with_alias.getQualifiedNamePrefix() + column.name;
                    joined_column.name_and_type.name = qualified_name;
                }

                /// We don't want to select duplicate columns from the joined subquery if they appear
                if (std::find(columns_from_joined_table.begin(), columns_from_joined_table.end(), joined_column) == columns_from_joined_table.end())
                    columns_from_joined_table.push_back(joined_column);

            }
        }
    }

    return columns_from_joined_table;
}

void AnalyzedJoin::calculateAvailableJoinedColumns(
        const NameSet & source_columns, const Context & context, const ASTSelectQuery * select_query_with_join, bool make_nullable)
{
    const auto & columns = getColumnsFromJoinedTable(source_columns, context, select_query_with_join);

    NameSet joined_columns;

    for (auto & column : columns)
    {
        auto & column_name = column.name_and_type.name;
        auto & column_type = column.name_and_type.type;
        auto & original_name = column.original_name;
        {
            if (joined_columns.count(column_name)) /// Duplicate columns in the subquery for JOIN do not make sense.
                continue;

            joined_columns.insert(column_name);

            auto type = make_nullable ? makeNullable(column_type) : column_type;
            available_joined_columns.emplace_back(NameAndTypePair(column_name, std::move(type)), original_name);
        }
    }
}


NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context)
{
    NamesAndTypesList names_and_type_list;
    if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        names_and_type_list = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context).getNamesAndTypesList();
    }
    else if (table_expression.table_function)
    {
        const auto table_function = table_expression.table_function;
        auto query_context = const_cast<Context *>(&context.getQueryContext());
        const auto & function_storage = query_context->executeTableFunction(table_function);
        names_and_type_list = function_storage->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }
    else if (table_expression.database_and_table_name)
    {
        DatabaseAndTableWithAlias database_table(table_expression.database_and_table_name);
        const auto & table = context.getTable(database_table.database, database_table.table);
        names_and_type_list = table->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }

    return names_and_type_list;
}

}
