#include <Storages/ColumnDefault.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>

namespace
{

struct AliasNames
{
    static constexpr const char * DEFAULT = "DEFAULT";
    static constexpr const char * MATERIALIZED = "MATERIALIZED";
    static constexpr const char * ALIAS = "ALIAS";
};

}

namespace DB
{

ColumnDefaultKind columnDefaultKindFromString(const std::string & str)
{
    static const std::unordered_map<std::string, ColumnDefaultKind> map{
        { AliasNames::DEFAULT, ColumnDefaultKind::Default },
        { AliasNames::MATERIALIZED, ColumnDefaultKind::Materialized },
        { AliasNames::ALIAS, ColumnDefaultKind::Alias }
    };

    const auto it = map.find(str);
    return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str};
}


std::string toString(const ColumnDefaultKind kind)
{
    static const std::unordered_map<ColumnDefaultKind, std::string> map{
        { ColumnDefaultKind::Default, AliasNames::DEFAULT },
        { ColumnDefaultKind::Materialized, AliasNames::MATERIALIZED },
        { ColumnDefaultKind::Alias, AliasNames::ALIAS }
    };

    const auto it = map.find(kind);
    return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultKind"};
}


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
    return lhs.kind == rhs.kind && queryToString(lhs.expression) == queryToString(rhs.expression);
}

}
