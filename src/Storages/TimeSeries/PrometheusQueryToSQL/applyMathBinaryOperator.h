#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether a specified string is the name of a prometheus math binary operator:
/// '+', '-', '*', '/', '%', '^', 'atan2'
bool isMathBinaryOperator(std::string_view operator_name);

/// Applies a prometheus math binary operator.
SQLQueryPiece applyMathBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

SQLQueryPiece applyMathLikeBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast);

}
