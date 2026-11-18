#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// The direction in which a PromQL sort function orders the samples of an instant vector.
enum class SortDirection : uint8_t
{
    ASCENDING,
    DESCENDING,
};

/// Returns the direction of the PromQL function `sort` or `sort_desc`, or `std::nullopt` for any other name.
std::optional<SortDirection> getSortDirection(std::string_view function_name);

/// Returns whether the specified string is the name of the PromQL function `sort` or `sort_desc`.
inline bool isFunctionSort(std::string_view function_name) { return getSortDirection(function_name).has_value(); }

/// Applies the PromQL function `sort` or `sort_desc`. The function returns its argument unchanged: the order is applied only
/// when `sort` or `sort_desc` is the root of an instant query, by `finalizeSQL`, because an ORDER BY in a subquery is not kept.
SQLQueryPiece applyFunctionSort(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
