#pragma once

#include <common/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ColumnsDescription;
ASTPtr replaceAliasColumnsInFilter(ASTPtr && ast, const ColumnsDescription & columns);

}
