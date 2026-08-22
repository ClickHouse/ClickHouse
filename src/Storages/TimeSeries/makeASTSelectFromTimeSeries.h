#pragma once

#include <Common/SettingsChanges.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class StorageTimeSeries;
struct SelectQueryInfo;

/// Builds an internal `SELECT` query that produces the requested outer columns of a TimeSeries
/// table by reading from its three target tables "tags", "samples", "metrics".
///
/// `requested_columns` is the set of outer columns the query needs (after column pruning); only the inner
/// tables those columns require are read. It may be empty — e.g. `SELECT count() FROM t`, which then reads the
/// "tags" table so the count equals the number of series — or the full set of outer columns (`SELECT *`).
///
/// `query_info` provides the query filter over the outer columns (`filter_actions_dag`, may be null) and the
/// query tree. Conditions on `metric_name` are pushed down onto the "tags" table read so its primary key can
/// skip granules. The query tree is also inspected to find whether the `tags` column is read only as
/// `tags['<const key>']`: when so, the `tags` column is built containing just those keys (resolved directly from
/// their sources) instead of reconstructing the full normalized Map.
ASTPtr makeASTSelectFromTimeSeries(
    const StorageTimeSeries & storage,
    const NameSet & requested_columns,
    const SelectQueryInfo & query_info,
    const ContextPtr & context);

/// The settings the generated read query must run with, independent of the caller's session/profile.
/// Apply them to the (child) context that runs `makeASTSelectFromTimeSeries`.
SettingsChanges getSettingsForSelectFromTimeSeries();

}
