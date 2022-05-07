#pragma once

#include <Parsers/IAST.h>

#include <string>
#include <unordered_map>


namespace DB
{

enum class ColumnDefaultKind
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
};


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);


using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;

}
