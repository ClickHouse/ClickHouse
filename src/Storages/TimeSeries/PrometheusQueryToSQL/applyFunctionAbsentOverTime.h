#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of the PromQL function `absent_over_time`.
inline bool isFunctionAbsentOverTime(std::string_view function_name)
{
    return function_name == "absent_over_time";
}

/// Applies the PromQL function `absent_over_time`.
///
/// `absent_over_time(v range-vector)` returns an empty vector if the range vector `v` has any
/// elements, and a 1-element vector with the value 1 if `v` has no elements. The labels of the
/// produced sample are derived from the input selector's matchers using the same "smart" label
/// derivation logic as `absent()`. If the input is not a bare selector (e.g. a subquery like
/// `absent_over_time(sum(nonexistent)[5m:])`), the produced sample has no labels (i.e. `{}`).
SQLQueryPiece applyFunctionAbsentOverTime(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
