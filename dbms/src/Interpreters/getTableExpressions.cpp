#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

NameSet removeDuplicateColumns(NamesAndTypesList & columns)
{
    NameSet names;
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (names.emplace(it->name).second)
            ++it;
        else
            columns.erase(it++);
    }
    return names;
}

std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select_query)
{
    if (!select_query.tables())
        return {};

    std::vector<const ASTTableExpression *> tables_expression;

    for (const auto & child : select_query.tables()->children)
    {
        const auto * tables_element = child->as<ASTTablesInSelectQueryElement>();

        if (tables_element->table_expression)
            tables_expression.emplace_back(tables_element->table_expression->as<ASTTableExpression>());
    }

    return tables_expression;
}

const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (!select.tables())
        return {};

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.size() <= table_number)
        return {};

    const auto & tables_element = tables_in_select_query.children[table_number]->as<ASTTablesInSelectQueryElement &>();

    if (!tables_element.table_expression)
        return {};

    return tables_element.table_expression->as<ASTTableExpression>();
}

ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number)
{
    if (const ASTTableExpression * table_expression = getTableExpression(select, table_number))
    {
        if (table_expression->database_and_table_name)
            return table_expression->database_and_table_name;

        if (table_expression->table_function)
            return table_expression->table_function;

        if (table_expression->subquery)
            return table_expression->subquery->children[0];
    }

    return nullptr;
}

NamesAndTypesList getColumnsFromTableExpression(const ASTTableExpression & table_expression, const Context & context)
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

std::vector<TableWithColumnNames> getDatabaseAndTablesWithColumnNames(const ASTSelectQuery & select_query, const Context & context)
{
    std::vector<TableWithColumnNames> tables_with_columns;

    if (select_query.tables() && !select_query.tables()->children.empty())
    {
        String current_database = context.getCurrentDatabase();

        for (const ASTTableExpression * table_expression : getTableExpressions(select_query))
        {
            DatabaseAndTableWithAlias table_name(*table_expression, current_database);

            NamesAndTypesList names_and_types = getColumnsFromTableExpression(*table_expression, context);
            removeDuplicateColumns(names_and_types);

            tables_with_columns.emplace_back(std::move(table_name), names_and_types.getNames());
        }
    }

    return tables_with_columns;
}

}
