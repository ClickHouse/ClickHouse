#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Makes a SQL query to return a scalar literal.
SQLQueryPiece fromLiteral(const PrometheusQueryTree::Scalar * scalar_node, ConverterContext & context);

/// Makes a SQL query to return a string literal.
SQLQueryPiece fromLiteral(const PrometheusQueryTree::StringLiteral * string_node, ConverterContext & context);

}
