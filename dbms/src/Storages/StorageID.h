#pragma once
#include <Core/Types.h>
#include <Common/quoteString.h>
#include <tuple>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

struct StorageID
{
    String database_name;
    String table_name;
    String uuid;

    //StorageID() = delete;
    StorageID() = default;

    //TODO StorageID(const ASTPtr & query_with_one_table, const Context & context) to get db and table names (and maybe uuid) from query
    //But there are a lot of different ASTs with db and table name
    //And it looks like it depends on https://github.com/ClickHouse/ClickHouse/pull/7774

    StorageID(const String & database, const String & table, const String & uuid_ = {})
            : database_name(database), table_name(table), uuid(uuid_)
    {
    }

    String getFullTableName() const
    {
        assert_valid();
        return (database_name.empty() ? "" : database_name + ".") + table_name;
    }

    String getNameForLogs() const
    {
        assert_valid();
        return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name) + (uuid.empty() ? "" : " (UUID " + uuid + ")");
    }

    String getId() const
    {
        //if (uuid.empty())
        return getFullTableName();
        //else
        //    return uuid;
    }

    bool operator<(const StorageID & rhs) const
    {
        assert_valid();
        /// It's needed for ViewDependencies
        if (uuid.empty() && rhs.uuid.empty())
            /// If both IDs don't have UUID, compare them like pair of strings
            return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
        else if (!uuid.empty() && !rhs.uuid.empty())
            /// If both IDs have UUID, compare UUIDs and ignore database and table name
            return uuid < rhs.uuid;
        else
            /// All IDs without UUID are less, then all IDs with UUID
            return uuid.empty();
    }

    bool empty() const
    {
        return table_name.empty() || (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && uuid.empty());
    }

    void assert_valid() const
    {
        if (empty())
            throw Exception("empty table name", ErrorCodes::LOGICAL_ERROR);
        if (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && uuid.empty() && !database_name.empty())
            throw Exception("unexpected database name", ErrorCodes::LOGICAL_ERROR);

    }
};

}
