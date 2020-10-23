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
    UUID uuid = UUID{UInt128(0, 0)};


    StorageID(const String & database, const String & table, UUID uuid_ = UUID{UInt128(0, 0)})
            : database_name(database), table_name(table), uuid(uuid_)
    {
        assertNotEmpty();
    }

    String getDatabaseName() const
    {
        assertNotEmpty();
        return database_name;
    }

    String getTableName() const
    {
        assertNotEmpty();
        return table_name;
    }

    String getFullTableName() const
    {
        assertNotEmpty();
        return (database_name.empty() ? "" : database_name + ".") + table_name;
    }

    String getNameForLogs() const
    {
        assertNotEmpty();
        return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name)
               + (hasUUID() ? " (UUID " + toString(uuid) + ")" : "");
    }

    bool empty() const
    {
        return table_name.empty() && !hasUUID();
    }

    bool hasUUID() const
    {
        return uuid != UUID{UInt128(0, 0)};
    }

    bool operator<(const StorageID & rhs) const
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

    void assertNotEmpty() const
    {
        if (empty())
            throw Exception("Both table name and UUID are empty", ErrorCodes::LOGICAL_ERROR);
        if (table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER && !hasUUID())
            throw Exception("Table name was replaced with placeholder, but UUID is Nil", ErrorCodes::LOGICAL_ERROR);
        if (table_name.empty() && !database_name.empty())
            throw Exception("Table name is empty, but database name is not", ErrorCodes::LOGICAL_ERROR);
    }

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

private:
    StorageID() = default;
};

}
