#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Drops the metric name (tag `__name__`) if not dropped before, as PromQL functions/operators almost always do.
/// Must not be called with StoreMethod::RAW_DATA or StoreMethod::HISTOGRAM_RAW_DATA.
SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context);

}
