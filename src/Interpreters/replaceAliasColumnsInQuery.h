#pragma once

#include <common/types.h>
#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ColumnsDescription;
class Context;
void replaceAliasColumnsInQuery(ASTPtr & ast, const ColumnsDescription & columns, const NameSet & forbidden_columns, const Context & context);

}
