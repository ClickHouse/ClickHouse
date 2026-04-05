#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns true if it's the name of a prometheus function taking a range vector,
/// for example rate(), idelta(), last_over_time().
bool isFunctionOverRange(const String & function_name);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const Node * node, const String & function_name, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
