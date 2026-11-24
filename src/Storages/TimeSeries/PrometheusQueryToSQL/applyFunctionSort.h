#pragma once

#include <Analyzer/SortNode.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// The struct holds what distinguishes the PromQL sort functions from each other.
struct SortFunctionInfo
{
    /// Name of the PromQL sort function.
    std::string_view function_name;

    /// Direction of the sort.
    SortDirection direction;

    /// Whether the sort is by label.
    bool by_label;
};

/// Returns whether the specified string is the name of a PromQL sort function: `sort`, `sort_desc`, `sort_by_label`
/// or `sort_by_label_desc`.
bool isFunctionSort(std::string_view function_name);

/// Returns the properties of the PromQL sort function with the specified name, or `std::nullopt` for any other name.
std::optional<SortFunctionInfo> getSortFunctionInfo(std::string_view function_name);

/// Applies a PromQL sort function. The function returns its argument unchanged, because SQL keeps the order of rows
/// only from the ORDER BY of the outermost SELECT: later steps (GROUP BY, joins) reorder the rows, so an ORDER BY in
/// a subquery would be lost or removed as redundant. The order is applied by `finalizeSQL`, with `getSortOrder`, and
/// only when the sort function is the root of an instant query.
SQLQueryPiece applyFunctionSort(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
