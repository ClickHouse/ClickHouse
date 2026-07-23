#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies a binary operator.
SQLQueryPiece applyBinaryOperator(
    const PQT::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context);

}
