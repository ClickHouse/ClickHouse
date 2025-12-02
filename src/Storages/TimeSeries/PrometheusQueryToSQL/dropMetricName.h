#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Drops the metric name (i.e. tag '__name__') if it hasn't been dropped before.
/// Prometheus functions and operators returning instant vectors almost always do that.
SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context);

}
