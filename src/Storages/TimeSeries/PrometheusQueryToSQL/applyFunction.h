#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies a prometheus function.
SQLQueryPiece applyFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
