#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether a specified string is the name of a prometheus math binary operator:
/// '+', '-', '*', '/', '%', '^', 'atan2'
bool isMathBinaryOperator(std::string_view operator_name);

/// Builds `f(x, y)` for a prometheus math binary operator.
ASTPtr applyMathBinaryOperatorToAST(std::string_view operator_name, ASTPtr x, ASTPtr y);

/// Applies a prometheus math binary operator.
SQLQueryPiece applyMathBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

}
