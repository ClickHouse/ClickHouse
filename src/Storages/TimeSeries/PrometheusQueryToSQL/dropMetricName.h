#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Drops the metric name (i.e. tag '__name__') if it hasn't been dropped before.
/// Prometheus functions and operators returning instant vectors almost always do that.
/// The removal is delayed: the series are only marked with `kDroppedMetricNameMarker` here,
/// and finalizeSQL removes the metric name from the final result.
/// The function must not be called with StoreMethod::RAW_DATA.
SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context);

}
