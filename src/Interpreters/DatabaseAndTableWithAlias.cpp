#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>

#include <Common/quoteString.h>
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
    extern const int INVALID_IDENTIFIER;
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableIdentifier & identifier, const String & current_database)
{
    alias = identifier.tryGetAlias();

    auto table_id = identifier.getTableId();
    std::tie(database, table, uuid) = std::tie(table_id.database_name, table_id.table_name, table_id.uuid);
    database_quote = table_id.database_name_quote;
    table_quote = table_id.table_name_quote;
    if (database.empty())
    {
        database = current_database;
        /// The implicit current database is canonical already; keep it exact.
        database_quote = IdentifierPartQuote::DoubleQuoted;
    }
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database)
{
    alias = identifier.tryGetAlias();

    if (identifier.name_parts.size() == 2)
    {
        std::tie(database, table) = std::tie(identifier.name_parts[0].spelling, identifier.name_parts[1].spelling);
        database_quote = identifier.name_parts[0].quote;
        table_quote = identifier.name_parts[1].quote;
    }
    else if (identifier.name_parts.size() == 1)
    {
        table = identifier.name_parts[0].spelling;
        table_quote = identifier.name_parts[0].quote;
    }
    else
        throw Exception(ErrorCodes::INVALID_IDENTIFIER, "Invalid identifier {}", backQuote(identifier.name()));

    if (database.empty())
    {
        database = current_database;
        database_quote = IdentifierPartQuote::DoubleQuoted;
    }
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTPtr & node, const String & current_database)
{
    if (const auto * table_identifier = node->as<ASTTableIdentifier>())
        *this = DatabaseAndTableWithAlias(*table_identifier, current_database);
    else if (const auto * identifier = node->as<ASTIdentifier>())
        *this = DatabaseAndTableWithAlias(*identifier, current_database);
    else
        throw Exception(ErrorCodes::INVALID_IDENTIFIER, "Identifier or table identifier expected");
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database)
{
    if (table_expression.database_and_table_name)
        *this = DatabaseAndTableWithAlias(table_expression.database_and_table_name, current_database);
    else if (table_expression.table_function)
        alias = table_expression.table_function->tryGetAlias();
    else if (table_expression.subquery)
    {
        const auto & cte_name = table_expression.subquery->as<const ASTSubquery &>().cte_name;
        if (!cte_name.empty())
        {
            database = current_database;
            table = cte_name;
        }
        alias = table_expression.subquery->tryGetAlias();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No known elements in ASTTableExpression");
}

void DatabaseAndTableWithAlias::resolveCanonicalNames(ContextPtr context)
{
    if (!context || table.empty())
        return;

    const bool implicit_database = database.empty();
    /// An unqualified name may refer to a temporary table, which resolves by exact name and must not fold.
    if (implicit_database && context->tryResolveStorageID(StorageID{"", table}, Context::ResolveExternal))
        return;

    String database_name = implicit_database ? context->getCurrentDatabase() : database;
    if (database_name.empty())
        return;

    StorageID table_id(database_name, table, uuid);
    /// The implicit current database is canonical already; keep it exact.
    table_id.database_name_quote = implicit_database ? IdentifierPartQuote::DoubleQuoted : database_quote;
    table_id.table_name_quote = table_quote;
    table_id = DatabaseCatalog::instance().resolveStorageIDNames(std::move(table_id), context);
    if (!implicit_database)
        database = table_id.database_name;
    table = table_id.table_name;
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
    if (!database_and_table_name || !database_and_table_name->as<ASTTableIdentifier>())
        return {};

    return DatabaseAndTableWithAlias(database_and_table_name);
}

}
