#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/StorageSnapshot.h>

#include <optional>

namespace DB
{

class ColumnsDescription;

/// columns the expression reads from a table with these columns; storage_snapshot, if set, is the real table,
/// whose name makes qualified names like t.col resolve and whose virtual columns, like _part, the expression may read;
/// table_id, if set, is the name to resolve instead, e.g. an `Alias` that reads the table
Names expressionSourceColumns(
    const ASTPtr & ast,
    const ColumnsDescription & columns,
    const ContextPtr & context,
    bool keep_alias_columns = false,
    const StorageSnapshotPtr & storage_snapshot = nullptr,
    const std::optional<StorageID> & table_id = {});

}
