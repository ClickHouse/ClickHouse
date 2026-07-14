#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

/// One matcher for `additional_table_filters` keys in both analyzers: the alias, the
/// canonical `db.table` name, the database-relative table name, and - under a selected
/// namespace - the name relative to that namespace all address the same table.
bool additionalTableFilterMatches(
    const String & filter_key, const String & table_expression_alias, const StorageID & storage_id, const Context & context);

}
