#pragma once

#include <Common/SettingsChanges.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class StorageTimeSeries;
struct SelectQueryInfo;

/// Builds an internal `SELECT` query that produces the requested outer columns of a TimeSeries table
/// by reading from its target tables "tags", "samples", "metrics". Only the target tables required by
/// `requested_columns` are read; when none of them requires another table (e.g. for `SELECT count()`
/// the planner requests just the smallest column, `metric_name`), the "tags" table is read, so the
/// count equals the number of series.
///
/// `query_info` provides the query tree (when the query and the filters applied outside it — a row
/// policy, `additional_table_filters` — use `tags` only as `tags['<const key>']`, a reduced `tags` Map
/// with just those keys is built) and the `FINAL` flag (with `FINAL` unmerged rows of the
/// "tags" table are deduplicated so a series is returned exactly once; without it a series may be
/// returned once per unmerged part, but the read is cheaper).
ASTPtr makeASTSelectFromTimeSeries(
    const StorageTimeSeries & storage,
    const NameSet & requested_columns,
    const SelectQueryInfo & query_info,
    const ContextPtr & context);

/// The settings the generated read query must run with, independent of the caller's session/profile.
/// Apply them to the (child) context that runs `makeASTSelectFromTimeSeries`.
/// `final` is whether the outer query uses the `FINAL` keyword (see `SelectQueryInfo::isFinal`).
SettingsChanges getSettingsForSelectFromTimeSeries(bool final);

}
