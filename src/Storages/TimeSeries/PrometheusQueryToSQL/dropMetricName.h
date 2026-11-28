#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Drops the metric name (i.e. tag '__name__') if it hasn't been dropped before.
/// Prometheus functions and operators returning instant vectors almost always do that.
/// The function must not be called with StoreMethod::SAMPLES or StoreMethod::TIMESTAMP_AND_VALUE.
/// If `query_piece.node` is a function or an operator whose only vector argument is a selector of one metric name
/// (see `cannotGetDuplicateSeries` in dropMetricName.cpp), the duplicate series check is skipped. So the caller must
/// guarantee that its result has one series per series of that argument with the same tags except `__name__`,
/// or that it regroups the series by the remaining tags (as `histogram_quantile` does).
SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context);

}
