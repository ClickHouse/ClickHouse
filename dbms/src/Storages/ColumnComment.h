#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>

namespace DB
{

struct ColumnComment
{
    ASTPtr expression;
};

bool operator== (const ColumnComment& lhs, const ColumnComment& rhs);

using ColumnComments = std::unordered_map<std::string, ColumnComment>;

}
