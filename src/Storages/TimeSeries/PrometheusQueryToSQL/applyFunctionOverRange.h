#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector.
/// Examples: rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// `drop_stale_markers_from_result` replaces Prometheus stale markers with NULL in the resulting grid.
/// It is used to build the grid of an instant selector: the raw samples of an instant selector keep the
/// stale markers on purpose (so that a stale marker hides the samples before it), but once the grid is
/// built a stale step simply means "the series is absent here" for every consumer of the grid.
SQLQueryPiece applyFunctionOverRange(
    const Node * node,
    std::string_view function_name,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context,
    bool drop_stale_markers_from_result = false);

}
