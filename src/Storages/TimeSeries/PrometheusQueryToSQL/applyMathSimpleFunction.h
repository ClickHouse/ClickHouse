#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether a specified string is the name of a math function.
/// The function only considers simple functions with one vector argument transforming each value separately.
/// Examples: abs(), floor(), sqrt(), sin(), etc.
bool isMathSimpleFunction(std::string_view function_name);

/// Applies a math function.
SQLQueryPiece applyMathSimpleFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
