#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ColumnsDescription;

/// columns the expression reads from a table with these columns; table_id makes qualified names like t.col resolve
Names expressionSourceColumns(
    const ASTPtr & ast,
    const ColumnsDescription & columns,
    const ContextPtr & context,
    bool keep_alias_columns = false,
    const StorageID & table_id = StorageID{"dummy", "dummy"});

}
