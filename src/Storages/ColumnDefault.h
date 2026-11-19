#pragma once

#include <Parsers/IAST_fwd.h>

#include <string>
#include <unordered_map>


namespace DB
{

enum class ColumnDefaultKind : uint8_t
{
    Default,
    Materialized,
    Alias,
    Ephemeral
};


ColumnDefaultKind columnDefaultKindFromString(const std::string & str);
std::string toString(ColumnDefaultKind kind);


struct ColumnDefault
{
    ColumnDefault() = default;
    ColumnDefault(const ColumnDefault & other) { *this = other; }
    ColumnDefault & operator=(const ColumnDefault & other);
    ColumnDefault(ColumnDefault && other) noexcept { *this = std::move(other); }
    ColumnDefault & operator=(ColumnDefault && other) noexcept;

    ColumnDefaultKind kind = ColumnDefaultKind::Default;
    ASTPtr expression;
    bool ephemeral_default = false;
};

bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);

using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;

}
