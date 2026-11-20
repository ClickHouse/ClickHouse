#pragma once

#include <Analyzer/SortNode.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// The struct holds the ORDER BY that a PromQL sort function at the root of an instant query puts on the result.
struct SortOrder
{
    /// Expressions over the columns `group`, `tags` and `value` of the final SELECT.
    ASTs keys;
    SortDirection direction;
};

/// Returns whether the specified string is the name of a PromQL sort function: `sort`, `sort_desc`, `sort_by_label`
/// or `sort_by_label_desc`.
bool isFunctionSort(std::string_view function_name);

/// Applies a PromQL sort function. The function returns its argument unchanged, because SQL keeps the order of rows
/// only from the ORDER BY of the outermost SELECT: later steps (GROUP BY, joins) reorder the rows, so an ORDER BY in
/// a subquery would be lost or removed as redundant. The order is applied by `finalizeSQL`, with `getSortOrder`, and
/// only when the sort function is the root of an instant query.
SQLQueryPiece applyFunctionSort(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// Returns the ORDER BY of the final SELECT for the specified result of an instant query, if the root of the query is
/// a PromQL sort function. Returns `std::nullopt` if the root is anything else, or if the result holds at most one
/// series, so there is nothing to order.
std::optional<SortOrder> getSortOrder(const SQLQueryPiece & result);

}
