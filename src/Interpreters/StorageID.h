#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>
#include <tuple>
#include <Parsers/IAST_fwd.h>
#include <Core/QualifiedTableName.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

class ASTQueryWithTableAndOutput;
class ASTIdentifier;
class Context;

struct StorageID
{
    String database_name;
    String table_name;
    UUID uuid = UUIDHelpers::Nil;

    StorageID(const String & database, const String & table, UUID uuid_ = UUIDHelpers::Nil)
        : database_name(database), table_name(table), uuid(uuid_)
    {
        assertNotEmpty();
    }

    StorageID(const ASTQueryWithTableAndOutput & query);
    StorageID(const ASTIdentifier & table_identifier_node);
    StorageID(const ASTPtr & node);

    String getDatabaseName() const;

    String getTableName() const;

    String getFullTableName() const;

    String getNameForLogs() const;

    operator bool () const
    {
        return !empty();
    }

    bool empty() const
    {
        return table_name.empty() && !hasUUID();
    }

    bool hasUUID() const
    {
        return uuid != UUIDHelpers::Nil;
    }

    bool operator<(const StorageID & rhs) const;

    void assertNotEmpty() const
    {
        // Can be triggered by user input, e.g. SELECT joinGetOrNull('', 'num', 500)
        if (empty())
            throw Exception("Table name cannot be empty. Please specify a valid table name or UUID", ErrorCodes::BAD_ARGUMENTS);

        // This can also be triggered by user input, but we haven't decided what
        // to do about it: create table "_"(a int) engine Log;
        if (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && !hasUUID())
            throw Exception("Table name was replaced with placeholder, but UUID is Nil", ErrorCodes::LOGICAL_ERROR);

        if (table_name.empty() && !database_name.empty())
            throw Exception("Table name is empty, but database name is not", ErrorCodes::LOGICAL_ERROR);
    }

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

    QualifiedTableName getQualifiedName() const { return {database_name, getTableName()}; }

private:
    StorageID() = default;
};

}
