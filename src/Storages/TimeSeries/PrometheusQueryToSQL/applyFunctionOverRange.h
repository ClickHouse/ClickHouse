#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns true if it's the name of a prometheus function taking a range vector.
bool isFunctionOverRange(const String & function_name);

/// Applies a prometheus function taking a range vector.
/// Supports functions like rate(), idelta(), last_over_time().
SQLQueryPiece applyFunctionOverRange(
    const Node * node, const String & function_name, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);
}
