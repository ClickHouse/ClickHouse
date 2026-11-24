#pragma once

#include <Analyzer/SortNode.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// The struct holds the ORDER BY of the final SELECT of an instant query.
struct SortOrder
{
    /// Expressions over the columns `group`, `tags` and `value` of the final SELECT.
    ASTs keys;
    SortDirection direction;
};

/// Returns the ORDER BY of the final SELECT for the specified result of an instant query, or `std::nullopt` if
/// the result is not ordered. The order depends on the root of the query:
/// - a PromQL sort function (see `applyFunctionSort`): by value or by the specified labels, if the result can hold
///   more than one series;
/// - `topk` or `bottomk`: by value, inside each bucket of `by` or `without`;
/// - anything else: by tags, if the result can hold more than one series.
std::optional<SortOrder> getSortOrder(const SQLQueryPiece & result);

}
