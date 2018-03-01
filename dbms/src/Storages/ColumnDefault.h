#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>


namespace DB
{

enum class ColumnDefaultType
{
    Default,
    Materialized,
    Alias
};


ColumnDefaultType columnDefaultTypeFromString(const std::string & str);
std::string toString(const ColumnDefaultType type);


struct ColumnDefault
{
    ColumnDefaultType type;
    ASTPtr expression;
};


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);


using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;


}
