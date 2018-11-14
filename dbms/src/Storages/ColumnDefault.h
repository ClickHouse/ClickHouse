#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>


namespace DB
{

class Context;
class Block;

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

/// Static methods to manipulate column defaults
struct ColumnDefaultsHelper
{
    static void attachFromContext(const Context & context, Block & sample);
    static ColumnDefaults extract(Block & sample);

    static ColumnDefaults loadFromContext(const Context & context, const String & database, const String & table);
    static ColumnDefaults loadFromContext(const Context & context); /// FIXME: we need another way to store current table
};

}
