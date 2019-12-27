#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
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
    UUID uuid;

    //StorageID() = delete;
    StorageID() = default;

    StorageID(const String & database, const String & table, UUID uuid_ = UUID{UInt128(0, 0)})
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
        return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name) + (hasUUID() ? "" : " (UUID " + toString(uuid) + ")");
    }

    bool operator<(const StorageID & rhs) const
    {
        assert_valid();
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

    bool empty() const
    {
        return table_name.empty() || (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && !hasUUID());
    }

    void assert_valid() const
    {
        if (empty())
            throw Exception("empty table name", ErrorCodes::LOGICAL_ERROR);
        if (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && !hasUUID() && !database_name.empty())
            throw Exception("unexpected database name", ErrorCodes::LOGICAL_ERROR);

    }

    bool hasUUID() const
    {
        return uuid != UUID{UInt128(0, 0)};
    }
};

}
