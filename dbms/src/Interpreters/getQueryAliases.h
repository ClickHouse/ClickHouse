#pragma once

#include <Parsers/IAST.h>
#include <unordered_map>

namespace DB
{

using Aliases = std::unordered_map<String, ASTPtr>;

void getQueryAliases(ASTPtr & ast, Aliases & aliases, int ignore_levels = 0);

}
