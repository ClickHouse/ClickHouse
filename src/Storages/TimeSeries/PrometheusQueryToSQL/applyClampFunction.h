#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether a specified string is the name of one of the clamping functions:
/// clamp(), clamp_min(), clamp_max().
bool isClampFunction(std::string_view function_name);

/// Applies one of the clamping functions: clamp(), clamp_min(), clamp_max().
SQLQueryPiece applyClampFunction(const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
