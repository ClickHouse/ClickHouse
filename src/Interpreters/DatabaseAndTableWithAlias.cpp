#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>

#include <Common/typeid_cast.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database)
{
    alias = identifier.tryGetAlias();

    auto table_id = IdentifierSemantic::extractDatabaseAndTable(identifier);
    std::tie(database, table, uuid) = std::tie(table_id.database_name, table_id.table_name, table_id.uuid);
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

bool DatabaseAndTableWithAlias::satisfies(const DatabaseAndTableWithAlias & db_table, bool table_may_be_an_alias) const
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

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database)
{
    std::vector<const ASTTableExpression *> tables_expression = getTableExpressions(select_query);

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

}
