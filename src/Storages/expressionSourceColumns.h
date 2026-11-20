#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

class ColumnsDescription;

/// columns the expression reads from a table with these columns; storage_snapshot, if set, is the real table,
/// whose name makes qualified names like t.col resolve and whose virtual columns, like _part, the expression may read
Names expressionSourceColumns(
    const ASTPtr & ast,
    const ColumnsDescription & columns,
    const ContextPtr & context,
    bool keep_alias_columns = false,
    const StorageSnapshotPtr & storage_snapshot = nullptr);

}
