#include <Interpreters/StorageID.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_DATABASE;
}

StorageID::StorageID(const ASTQueryWithTableAndOutput & query)
{
    database_name = query.getDatabase();
    table_name = query.getTable();
    uuid = query.uuid;
    assertNotEmpty();
}

StorageID::StorageID(const ASTTableIdentifier & table_identifier_node)
{
    DatabaseAndTableWithAlias database_table(table_identifier_node);
    database_name = database_table.database;
    table_name = database_table.table;
    uuid = database_table.uuid;
    assertNotEmpty();
}

StorageID::StorageID(const ASTPtr & node)
{
    if (const auto * identifier = node->as<ASTTableIdentifier>())
        *this = StorageID(*identifier);
    else if (const auto * simple_query = dynamic_cast<const ASTQueryWithTableAndOutput *>(node.get()))
        *this = StorageID(*simple_query);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST");
}

String StorageID::getTableName() const
{
    assertNotEmpty();
    return table_name;
}

String StorageID::getDatabaseName() const
{
    assertNotEmpty();
    if (database_name.empty())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database name is empty");
    return database_name;
}

String StorageID::getNameForLogs() const
{
    assertNotEmpty();
    return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name)
           + (hasUUID() ? " (" + toString(uuid) + ")" : "");
}

/// NOTE: This implementation doesn't allow to implement a good "operator <".
/// Because "a != b" must be equivalent to "(a < b) || (b < a)", and we can't make "operator <" to meet that.
bool StorageID::operator==(const StorageID & rhs) const
{
    assertNotEmpty();
    if (hasUUID() && rhs.hasUUID())
        return uuid == rhs.uuid;
    return std::tie(database_name, table_name) == std::tie(rhs.database_name, rhs.table_name);
}

String StorageID::getFullTableName() const
{
    return backQuoteIfNeed(getDatabaseName()) + "." + backQuoteIfNeed(table_name);
}

String StorageID::getFullNameNotQuoted() const
{
    return getDatabaseName() + "." + table_name;
}

StorageID StorageID::fromDictionaryConfig(const Poco::Util::AbstractConfiguration & config,
                                          const String & config_prefix)
{
    StorageID res = StorageID::createEmpty();
    res.database_name = config.getString(config_prefix + ".database", "");
    res.table_name = config.getString(config_prefix + ".name");
    const String uuid_str = config.getString(config_prefix + ".uuid", "");
    if (!uuid_str.empty())
        res.uuid = parseFromString<UUID>(uuid_str);
    return res;
}

String StorageID::getShortName() const
{
    assertNotEmpty();
    if (hasUUID())
        return toString(uuid);
    if (database_name.empty())
        return table_name;
    return database_name + "." + table_name;
}

}
