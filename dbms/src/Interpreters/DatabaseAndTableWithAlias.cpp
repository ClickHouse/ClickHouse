#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/AnalyzedJoin.h> /// for getNamesAndTypeListFromTableExpression
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

NameSet removeDuplicateColumns(NamesAndTypesList & columns);


DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database)
{
    alias = identifier.tryGetAlias();

    std::tie(database, table) = IdentifierSemantic::extractDatabaseAndTable(identifier);
    if (database.empty())
        database = current_database;
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTPtr & node, const String & current_database)
{
    const auto * identifier = node->as<ASTIdentifier>();
    if (!identifier)
        throw Exception("Logical error: identifier expected", ErrorCodes::LOGICAL_ERROR);

    *this = DatabaseAndTableWithAlias(*identifier, current_database);
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database)
{
    if (table_expression.database_and_table_name)
        *this = DatabaseAndTableWithAlias(table_expression.database_and_table_name, current_database);
    else if (table_expression.table_function)
        alias = table_expression.table_function->tryGetAlias();
    else if (table_expression.subquery)
        alias = table_expression.subquery->tryGetAlias();
    else
        throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);
}

bool DatabaseAndTableWithAlias::satisfies(const DatabaseAndTableWithAlias & db_table, bool table_may_be_an_alias)
{
    /// table.*, alias.* or database.table.*

    if (database.empty())
    {
        if (!db_table.table.empty() && table == db_table.table)
            return true;

        if (!db_table.alias.empty())
            return (alias == db_table.alias) || (table_may_be_an_alias && table == db_table.alias);
    }

    return database == db_table.database && table == db_table.table;
}

String DatabaseAndTableWithAlias::getQualifiedNamePrefix(bool with_dot) const
{
    if (alias.empty() && table.empty())
        return "";
    return (!alias.empty() ? alias : table) + (with_dot ? "." : "");
}

std::vector<const ASTTableExpression *> getSelectTablesExpression(const ASTSelectQuery & select_query)
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

static const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number)
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

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database)
{
    std::vector<const ASTTableExpression *> tables_expression = getSelectTablesExpression(select_query);

    std::vector<DatabaseAndTableWithAlias> database_and_table_with_aliases;
    database_and_table_with_aliases.reserve(tables_expression.size());

    for (const auto & table_expression : tables_expression)
        database_and_table_with_aliases.emplace_back(DatabaseAndTableWithAlias(*table_expression, current_database));

    return database_and_table_with_aliases;
}

std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number)
{
    const ASTTableExpression * table_expression = getTableExpression(select, table_number);
    if (!table_expression)
        return {};

    ASTPtr database_and_table_name = table_expression->database_and_table_name;
    if (!database_and_table_name || !database_and_table_name->as<ASTIdentifier>())
        return {};

    return DatabaseAndTableWithAlias(database_and_table_name);
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

std::vector<TableWithColumnNames> getDatabaseAndTablesWithColumnNames(const ASTSelectQuery & select_query, const Context & context)
{
    std::vector<TableWithColumnNames> tables_with_columns;

    if (select_query.tables() && !select_query.tables()->children.empty())
    {
        String current_database = context.getCurrentDatabase();

        for (const ASTTableExpression * table_expression : getSelectTablesExpression(select_query))
        {
            DatabaseAndTableWithAlias table_name(*table_expression, current_database);

            NamesAndTypesList names_and_types = getNamesAndTypeListFromTableExpression(*table_expression, context);
            removeDuplicateColumns(names_and_types);

            tables_with_columns.emplace_back(std::move(table_name), names_and_types.getNames());
        }
    }

    return tables_with_columns;
}

}
