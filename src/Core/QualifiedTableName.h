#pragma once

#include <base/types.h>

#include <optional>
#include <string>
#include <tuple>
#include <vector>


namespace DB
{

//TODO replace with StorageID
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
        return std::forward_as_tuple(database, table) < std::forward_as_tuple(other.database, other.table);
    }

    UInt64 hash() const;

    std::vector<std::string> getParts() const
    {
        if (database.empty())
            return {table};
        return {database, table};
    }

    std::string getFullName() const;

    /// NOTE: It's different from compound identifier parsing and does not support escaping and dots in name.
    /// Usually it's better to use ParserIdentifier instead,
    /// but we parse DDL dictionary name (and similar things) this way for historical reasons.
    static std::optional<QualifiedTableName> tryParseFromString(const String & maybe_qualified_name);
    static QualifiedTableName parseFromString(const String & maybe_qualified_name);
};

}

namespace std
{

template <> struct hash<DB::QualifiedTableName>
{
    using argument_type = DB::QualifiedTableName;
    using result_type = size_t;

    result_type operator()(const argument_type & qualified_table) const
    {
        return qualified_table.hash();
    }
};

}
