#pragma once

#include <Common/Exception.h>
#include <base/defines.h>
#include <base/types.h>
#include <boost/algorithm/string.hpp>

namespace DB
{
    template <typename Query>
    inline std::pair<String, String> parseDatabaseAndTableName(const Query & query, const String & current_database)
    {
        String database;
        String table;

        if (query.from_table.contains("."))
        {
            /// FROM <db>.<table> (abbreviated form)
            chassert(query.from_database.empty());
            std::vector<String> split;
            boost::split(split, query.from_table, boost::is_any_of("."));
            chassert(split.size() == 2);
            database = split[0];
            table = split[1];
        }
        else if (query.from_database.empty())
        {
            /// FROM <table>
            chassert(!query.from_table.empty());
            database = current_database;
            table = query.from_table;
        }
        else
        {
            /// FROM <database> FROM <table>
            chassert(!query.from_database.empty());
            chassert(!query.from_table.empty());
            database = query.from_database;
            table = query.from_table;
        }

        return {database, table};
    }
}
