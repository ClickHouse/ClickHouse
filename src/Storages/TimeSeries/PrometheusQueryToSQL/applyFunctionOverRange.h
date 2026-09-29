#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector.
/// Examples: rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Returns whether the specified range vector is the argument of a function over range calculated without extrapolation
/// (see setting `promql_exact_rate`). Such a function also uses the last sample before each window, if it is not older
/// than the window before the window's start, so the range vector must include one more window of samples.
bool needsSamplesBeforeWindow(const Node * range_vector_node, const ConverterContext & context);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// `drop_metric_name` overrides the function's own metric-name policy. Internal callers that reuse a
/// translation for a private intermediate (e.g. absent_over_time's presence grid) pass `false`: dropping
/// the name there could only manufacture duplicate label sets, which the public path rejects.
SQLQueryPiece applyFunctionOverRange(
    const Node * node,
    std::string_view function_name,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context,
    std::optional<bool> drop_metric_name = std::nullopt);

}
