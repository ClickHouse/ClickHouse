#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies a simple function taking arguments of types scalar or instant vector and
/// transforming each value separately.
/// Examples: math functions (e.g. abs(), sin()), date/time functions (e.g. day_of_week()),
/// binary operators if at least one of the operands is scalar.
///
/// applySimpleFunctionHelper() evaluates a simple function either directly - for example "SELECT sin(value) AS value",
/// or via arrayMap - for example "SELECT arrayMap(x -> sin(x), values) AS values".
///
/// applySimpleFunctionHelper() doesn't check argument types and doesn't drop the metric name.
SQLQueryPiece applySimpleFunctionHelper(
    const PQT::Node * node,
    ConverterContext & context,
    const std::function<ASTPtr(ASTs)> & apply_function_to_ast,
    std::vector<SQLQueryPiece> && arguments);

}
