#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Applies the specified unary operator (either '+' or '-').
SQLQueryPiece applyUnaryOperator(const PQT::UnaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context);

}
