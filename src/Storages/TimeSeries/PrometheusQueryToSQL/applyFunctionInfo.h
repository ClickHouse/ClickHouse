#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of the PromQL function info().
bool isFunctionInfo(std::string_view function_name);

/// Applies the PromQL function info(v, [label-selector]): adds the data labels of the info series, by default `target_info`,
/// which have the same `instance` and `job` labels as a series of `v`. Matches Prometheus 3.5.
SQLQueryPiece applyFunctionInfo(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
