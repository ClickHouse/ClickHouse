#pragma once

#include <string>
#include <tuple>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <fmt/format.h>

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

namespace fmt
{
    template <>
    struct formatter<DB::QualifiedTableName>
    {
        constexpr auto parse(format_parse_context & ctx)
        {
            return ctx.begin();
        }

        template <typename FormatContext>
        auto format(const DB::QualifiedTableName & name, FormatContext & ctx)
        {
            return format_to(ctx.out(), "{}.{}", DB::backQuoteIfNeed(name.database), DB::backQuoteIfNeed(name.table));
        }
    };
}

