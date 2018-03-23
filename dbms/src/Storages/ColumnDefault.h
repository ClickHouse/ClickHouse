#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>


namespace DB
{

enum class ColumnDefaultKind
{
    Default,
    Materialized,
    Alias
};


ColumnDefaultKind columnDefaultKindFromString(const std::string & str);
std::string toString(const ColumnDefaultKind type);


struct ColumnDefault
{
    ColumnDefaultKind kind;
    ASTPtr expression;
};


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);


using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;


}
