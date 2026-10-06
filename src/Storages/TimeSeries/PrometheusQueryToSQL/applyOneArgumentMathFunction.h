#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether a specified string is the name of a one-argument math function.
/// Examples: abs(), floor(), sqrt(), sin(), etc.
bool isOneArgumentMathFunction(std::string_view function_name);

/// Applies a one-argument math function.
SQLQueryPiece applyOneArgumentMathFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
