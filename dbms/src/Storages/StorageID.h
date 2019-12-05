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

struct StorageID
{
    String database_name;
    String table_name;
    String uuid;

    StorageID() = delete;

    StorageID(const String & database, const String & table, const String & uuid_ = {})
            : database_name(database), table_name(table), uuid(uuid_)
    {
        assert_not_empty();
    }

    String getFullTableName() const
    {
        return (database_name.empty() ? "" : database_name + ".") + table_name;
    }

    String getNameForLogs() const
    {
        return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name) + " (UUID " + uuid + ")";
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
        return std::tie(uuid, database_name, table_name) < std::tie(rhs.uuid, rhs.database_name, rhs.table_name);
    }

    void assert_not_empty() const
    {
        if (database_name.empty() && table_name.empty())
            throw Exception("empty table name", ErrorCodes::LOGICAL_ERROR);
    }
};

}
