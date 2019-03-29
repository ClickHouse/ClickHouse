#include <Storages/ColumnDefault.h>
#include <Parsers/queryToString.h>

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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ColumnDefaultKind columnDefaultKindFromString(const std::string & str)
{
    static const std::unordered_map<std::string, ColumnDefaultKind> map{
        { AliasNames::DEFAULT, ColumnDefaultKind::Default },
        { AliasNames::MATERIALIZED, ColumnDefaultKind::Materialized },
        { AliasNames::ALIAS, ColumnDefaultKind::Alias }
    };

    const auto it = map.find(str);
    return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str, ErrorCodes::LOGICAL_ERROR};
}


std::string toString(const ColumnDefaultKind kind)
{
    static const std::unordered_map<ColumnDefaultKind, std::string> map{
        { ColumnDefaultKind::Default, AliasNames::DEFAULT },
        { ColumnDefaultKind::Materialized, AliasNames::MATERIALIZED },
        { ColumnDefaultKind::Alias, AliasNames::ALIAS }
    };

    const auto it = map.find(kind);
    return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultKind", ErrorCodes::LOGICAL_ERROR};
}


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
    auto expression_str = [](const ASTPtr & expr) { return expr ? queryToString(expr) : String(); };
    return lhs.kind == rhs.kind && expression_str(lhs.expression) == expression_str(rhs.expression);
}

}
