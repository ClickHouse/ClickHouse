#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of the PromQL function timestamp().
bool isFunctionTimestamp(std::string_view function_name);

/// Applies the PromQL function timestamp(): returns the timestamp (in seconds since epoch) of the sample
/// selected for its argument at each point of the query's grid.
///
/// Supported argument shapes match Prometheus 3.5.0: a bare instant selector, optionally wrapped in a direct
/// offset or @ modifier (e.g. `timestamp(some_metric)`, `timestamp(some_metric offset 1m)`, `timestamp(some_metric @ 120)`).
/// Any other expression shape (vector-scalar arithmetic, unary operators, comparisons, nested timestamp() calls,
/// aggregations, or binary vector operations) throws NOT_IMPLEMENTED.
SQLQueryPiece applyFunctionTimestamp(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
