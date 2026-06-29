#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <optional>
#include <vector>


namespace DB
{
class StorageTimeSeries;
class ActionsDAG;

/// Builds an internal `SELECT` query that produces the requested outer columns of a TimeSeries
/// table by reading from its three target tables "tags", "samples", "metrics".
///
/// `filter_actions_dag` is the query filter over the outer columns (may be null). Conditions on
/// `metric_name` are pushed down onto the "tags" table read so its primary key can skip granules.
///
/// `projected_tag_keys`, when set, lists the only tag keys the query reads from the `tags` column (i.e. the
/// query uses `tags` solely as `tags['<const key>']`). The `tags` column is then built containing just those
/// keys, resolved directly from their sources, instead of reconstructing the full normalized Map. When unset
/// the full Map is built.
ASTPtr makeTimeSeriesReadQuery(
    const StorageTimeSeries & storage,
    const Names & column_names,
    const ContextPtr & context,
    const ActionsDAG * filter_actions_dag,
    const std::optional<std::vector<String>> & projected_tag_keys = {});

}
