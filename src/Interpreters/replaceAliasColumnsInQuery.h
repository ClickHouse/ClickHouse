#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <common/types.h>

namespace DB
{

class ColumnsDescription;

void replaceAliasColumnsInQuery(
    ASTPtr & ast, const ColumnsDescription & columns, const NameToNameMap & array_join_result_to_source, ContextPtr context);

}
