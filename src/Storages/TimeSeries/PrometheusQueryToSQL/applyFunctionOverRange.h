#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector.
/// Examples: rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Returns whether the specified argument of a prometheus function taking a range vector is a scalar expression which must be
/// converted keeping scalar values in `Float64` regardless of the scalar data type of the `TimeSeries` table.
/// Example: the second argument of predict_linear() is a prediction offset in seconds which may depend on the evaluation time
/// (e.g. `predict_linear(m[5m], time() - 1700000000)`); on a table with `Float32` values the evaluation timestamps would
/// otherwise be rounded to the coarse `Float32` granularity before the offsets are computed, so distinct steps would collapse.
bool isFunctionOverRangeFloat64ScalarArgument(std::string_view function_name, size_t argument_index);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

SQLQueryPiece applyFunctionOverRange(
    const Node * node, std::string_view function_name, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
