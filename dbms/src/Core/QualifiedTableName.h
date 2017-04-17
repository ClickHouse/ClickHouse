#pragma once

#include <string>
#include <Common/SipHash.h>

namespace DB
{

struct QualifiedTableName
{
    std::string database;
    std::string table;

    bool operator==(const QualifiedTableName & other) const
    {
        return database == other.database && table == other.table;
    }

    bool operator<(const QualifiedTableName & other) const
    {
        if (database == other.database)
            return table < other.table;
        return database < other.database;
    }

    UInt64 hash() const
    {
        SipHash hash_state;
        hash_state.update(database.data(), database.size());
        hash_state.update(table.data(), table.size());
        return hash_state.get64();
    }
};

}

namespace std
{

template<> struct hash<DB::QualifiedTableName>
{
    typedef DB::QualifiedTableName argument_type;
    typedef std::size_t result_type;

    result_type operator()(const argument_type & qualified_table) const
    {
        return qualified_table.hash();
    }
};

}
