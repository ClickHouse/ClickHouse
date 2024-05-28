#include <Storages/ColumnDefault.h>
#include <Parsers/queryToString.h>

namespace
{

struct AliasNames
{
    static constexpr const char * DEFAULT = "DEFAULT";
    static constexpr const char * MATERIALIZED = "MATERIALIZED";
    static constexpr const char * ALIAS = "ALIAS";
    static constexpr const char * EPHEMERAL = "EPHEMERAL";
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
        { AliasNames::ALIAS, ColumnDefaultKind::Alias },
        { AliasNames::EPHEMERAL, ColumnDefaultKind::Ephemeral }
    };

    const auto it = map.find(str);
    if (it != std::end(map))
        return it->second;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column default specifier: {}", str);
}


std::string toString(const ColumnDefaultKind kind)
{
    static const std::unordered_map<ColumnDefaultKind, std::string> map{
        { ColumnDefaultKind::Default, AliasNames::DEFAULT },
        { ColumnDefaultKind::Materialized, AliasNames::MATERIALIZED },
        { ColumnDefaultKind::Alias, AliasNames::ALIAS },
        { ColumnDefaultKind::Ephemeral, AliasNames::EPHEMERAL }
    };

    const auto it = map.find(kind);
    if (it != std::end(map))
        return it->second;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid ColumnDefaultKind");
}

ColumnDefault & ColumnDefault::operator=(const ColumnDefault & other)
{
    if (this == &other)
        return *this;

    kind = other.kind;
    expression = other.expression ? other.expression->clone() : nullptr;
    ephemeral_default = other.ephemeral_default;

    return *this;
}

ColumnDefault & ColumnDefault::operator=(ColumnDefault && other) noexcept
{
    if (this == &other)
        return *this;

    kind = std::exchange(other.kind, ColumnDefaultKind{});
    expression = other.expression ? other.expression->clone() : nullptr;
    other.expression.reset();
    ephemeral_default = std::exchange(other.ephemeral_default, false);

    return *this;
}

bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
    auto expression_str = [](const ASTPtr & expr) { return expr ? queryToString(expr) : String(); };
    return lhs.kind == rhs.kind && expression_str(lhs.expression) == expression_str(rhs.expression);
}

}
