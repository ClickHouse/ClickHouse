#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Controls how duplicate labelsets are handled after the metric name is removed.
enum class DuplicateSeriesHandling
{
    THROW,
    MERGE_NON_OVERLAPPING,
};

/// Drops the metric name (i.e. tag '__name__') if it hasn't been dropped before.
/// Prometheus functions and operators returning instant vectors almost always do that.
/// The function must not be called with StoreMethod::RAW_DATA.
/// By default duplicate labelsets are rejected; callers can request merging of non-overlapping samples.
SQLQueryPiece dropMetricName(
    SQLQueryPiece && query_piece,
    ConverterContext & context,
    DuplicateSeriesHandling duplicate_series_handling = DuplicateSeriesHandling::THROW);

}
