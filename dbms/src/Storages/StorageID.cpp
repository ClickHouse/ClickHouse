#include <Storages/StorageID.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

namespace DB
{

StorageID::StorageID(const ASTQueryWithTableAndOutput & query, const Context & local_context)
{
    database_name = local_context.resolveDatabase(query.database);
    table_name = query.table;
    uuid = query.uuid;
    assertNotEmpty();
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

}
