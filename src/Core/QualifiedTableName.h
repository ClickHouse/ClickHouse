#pragma once

#include <string>
#include <tuple>
#include <optional>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

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

    std::vector<std::string> getParts() const
    {
        if (database.empty())
            return {table};
        else
            return {database, table};
    }

    std::string getFullName() const
    {
        if (database.empty())
            return table;
        else
            return database + '.' + table;
    }

    /// NOTE: It's different from compound identifier parsing and does not support escaping and dots in name.
    /// Usually it's better to use ParserIdentifier instead,
    /// but we parse DDL dictionary name (and similar things) this way for historical reasons.
    static std::optional<QualifiedTableName> tryParseFromString(const String & maybe_qualified_name)
    {
        if (maybe_qualified_name.empty())
            return {};

        /// Do not allow dot at the beginning and at the end
        auto pos = maybe_qualified_name.find('.');
        if (pos == 0 || pos == (maybe_qualified_name.size() - 1))
            return {};

        QualifiedTableName name;
        if (pos == std::string::npos)
        {
            name.table = maybe_qualified_name;
        }
        else if (maybe_qualified_name.find('.', pos + 1) != std::string::npos)
        {
            /// Do not allow multiple dots
            return {};
        }
        else
        {
            name.database = maybe_qualified_name.substr(0, pos);
            name.table = maybe_qualified_name.substr(pos + 1);
        }

        return name;
    }

    static QualifiedTableName parseFromString(const String & maybe_qualified_name)
    {
        auto name = tryParseFromString(maybe_qualified_name);
        if (!name)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid qualified name: {}", maybe_qualified_name);
        return *name;
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
        static constexpr auto parse(format_parse_context & ctx)
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

