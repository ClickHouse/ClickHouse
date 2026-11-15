#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of the PromQL function `absent` or `absent_over_time`.
inline bool isFunctionAbsent(std::string_view function_name)
{
    return function_name == "absent" || function_name == "absent_over_time";
}

/// Applies the PromQL function `absent` or `absent_over_time`.
///
/// `absent(v instant-vector)` returns an empty vector if `v` has any elements, and a 1-element vector with the value 1
/// if `v` has no elements. `absent_over_time(v range-vector)` does the same over a range window: at each evaluation
/// step it checks whether any series of `v` has a sample in the window.
///
/// The tags of the produced sample are derived from the equality matchers of the input selector. If the input is not
/// a bare selector (in particular for any subquery, e.g. `absent_over_time(nonexistent[5m:1m])`), the produced sample
/// has no tags (i.e. `{}`).
SQLQueryPiece applyFunctionAbsent(const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
