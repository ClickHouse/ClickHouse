#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks if the specified string is the name of the `predict_linear` function.
/// `predict_linear` is special among range-vector functions because it takes two arguments:
/// a range vector and a scalar (the prediction horizon in seconds). It therefore cannot be
/// dispatched through `applyFunctionOverRange` (which handles single-argument range functions).
bool isFunctionPredictLinear(std::string_view function_name);

/// Applies the `predict_linear(v range-vector, t scalar)` function.
/// It performs simple linear regression over the samples in the range vector and predicts
/// the value `t` seconds from the last timestamp. The metric name is dropped.
SQLQueryPiece applyFunctionPredictLinear(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
