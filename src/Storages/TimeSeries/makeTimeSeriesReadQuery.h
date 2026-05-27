#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class StorageTimeSeries;

/// Builds an internal `SELECT` query that produces the requested outer columns of a TimeSeries
/// table by reading from its three target tables "tags", "samples", "metrics".
ASTPtr makeTimeSeriesReadQuery(
    const StorageTimeSeries & storage,
    const Names & column_names,
    const ContextPtr & context);

}
