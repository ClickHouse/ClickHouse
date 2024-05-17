#pragma once

#include <Parsers/IAST.h>

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
    ColumnDefaultKind kind = ColumnDefaultKind::Default;
    ASTPtr expression;
    bool ephemeral_default = false;
};


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);


using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;

}
