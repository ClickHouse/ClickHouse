#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of the PromQL function timestamp().
bool isFunctionTimestamp(std::string_view function_name);

/// Applies the PromQL function timestamp(): returns the timestamp (in seconds since epoch) of the sample
/// selected for its argument at each point of the query's grid.
///
/// Matching Prometheus 3.5.0 semantics:
/// - For direct instant vector selectors (e.g. `timestamp(test)`, `timestamp(test offset 1m)`, `timestamp(test @ 120)`),
///   returns the raw sample timestamp from storage.
/// - For general instant vector expressions (e.g. `timestamp(test * 1)`, `timestamp(-test)`, `timestamp(timestamp(test))`,
///   `timestamp(test > bool 10)`), returns the query step evaluation timestamp (T_eval) for each present sample.
SQLQueryPiece applyFunctionTimestamp(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
