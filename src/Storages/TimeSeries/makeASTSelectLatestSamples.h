#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <base/Decimal.h>


namespace DB
{

/// Builds a `SELECT` query returning the latest sample of each time series matched by any of the `selectors`
/// (instant selectors, e.g. `up{job="a"}`) among the samples in the range `(time - lookback_delta, time]`.
///
/// The series are grouped by their tags, so a series matched by several selectors, or stored under several identifiers
/// with the same tags, is returned once. The query returns the columns:
///   `tags`           - Array(Tuple(String, String)), the tags of the series;
///   `last_value`     - Nullable(Float64), the value of the latest sample;
///   `last_timestamp` - Nullable(DateTime64(3)), the timestamp of the latest sample.
/// A series whose latest sample is a stale marker is not returned. Rows are sorted by the metric name, then by the tags.
///
/// `time` and `lookback_delta` are expressed with the decimal scale `time_scale`.
ASTPtr makeASTSelectLatestSamples(
    const StorageID & time_series_storage_id,
    const Strings & selectors,
    DateTime64 time,
    Decimal64 lookback_delta,
    UInt32 time_scale);

}
