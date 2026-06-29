#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class StorageTimeSeries;
class ActionsDAG;

/// Builds an internal `SELECT` query that produces the requested outer columns of a TimeSeries
/// table by reading from its three target tables "tags", "samples", "metrics".
///
/// `filter_actions_dag` is the query filter over the outer columns (may be null). Conditions on
/// `metric_name` are pushed down onto the "tags" table read so its primary key can skip granules.
ASTPtr makeTimeSeriesReadQuery(
    const StorageTimeSeries & storage,
    const Names & column_names,
    const ContextPtr & context,
    const ActionsDAG * filter_actions_dag);

}
