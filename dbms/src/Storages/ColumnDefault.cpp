#include <Parsers/queryToString.h>
#include <Storages/ColumnDefault.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ColumnDefaultKind columnDefaultKindFromString(const std::string & str)
{
    static const std::unordered_map<std::string, ColumnDefaultKind> map{
        { "DEFAULT", ColumnDefaultKind::Default },
        { "MATERIALIZED", ColumnDefaultKind::Materialized },
        { "ALIAS", ColumnDefaultKind::Alias }
    };

    const auto it = map.find(str);
    return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str, ErrorCodes::LOGICAL_ERROR};
}


std::string toString(const ColumnDefaultKind kind)
{
    static const std::unordered_map<ColumnDefaultKind, std::string> map{
        { ColumnDefaultKind::Default, "DEFAULT" },
        { ColumnDefaultKind::Materialized, "MATERIALIZED" },
        { ColumnDefaultKind::Alias, "ALIAS" }
    };

    const auto it = map.find(kind);
    return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultKind", ErrorCodes::LOGICAL_ERROR};
}


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
    return lhs.kind == rhs.kind && queryToString(lhs.expression) == queryToString(rhs.expression);
}

}
