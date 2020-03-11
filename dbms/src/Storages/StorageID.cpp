#include <Storages/StorageID.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE;
}

StorageID::StorageID(const ASTQueryWithTableAndOutput & query, const Context & local_context)
{
    database_name = local_context.resolveDatabase(query.database);
    table_name = query.table;
    uuid = query.uuid;
    assertNotEmpty();
}

String StorageID::getDatabaseName() const
{
    assertNotEmpty();
    if (database_name.empty())
        throw Exception("Database name is empty", ErrorCodes::UNKNOWN_DATABASE);
    return database_name;
}

String StorageID::getNameForLogs() const
{
    assertNotEmpty();
    return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name)
           + (hasUUID() ? " (UUID " + toString(uuid) + ")" : "");
}

bool StorageID::operator<(const StorageID & rhs) const
{
    assertNotEmpty();
    /// It's needed for ViewDependencies
    if (!hasUUID() && !rhs.hasUUID())
        /// If both IDs don't have UUID, compare them like pair of strings
        return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
    else if (hasUUID() && rhs.hasUUID())
        /// If both IDs have UUID, compare UUIDs and ignore database and table name
        return uuid < rhs.uuid;
    else
        /// All IDs without UUID are less, then all IDs with UUID
        return !hasUUID();
}

void StorageID::assertNotEmpty() const
{
    if (empty())
        throw Exception("Both table name and UUID are empty", ErrorCodes::LOGICAL_ERROR);
    if (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && !hasUUID())
        throw Exception("Table name was replaced with placeholder, but UUID is Nil", ErrorCodes::LOGICAL_ERROR);
    if (table_name.empty() && !database_name.empty())
        throw Exception("Table name is empty, but database name is not", ErrorCodes::LOGICAL_ERROR);
}

StorageID StorageID::resolveFromAST(const ASTPtr & table_identifier_node, const Context & context)
{
    DatabaseAndTableWithAlias database_table(table_identifier_node);
    return context.tryResolveStorageID({database_table.database, database_table.table});
}

String StorageID::getFullTableName() const
{
    return backQuoteIfNeed(getDatabaseName()) + "." + backQuoteIfNeed(table_name);
}

}
