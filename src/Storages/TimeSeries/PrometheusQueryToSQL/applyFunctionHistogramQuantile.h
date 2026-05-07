#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

bool isFunctionHistogramQuantile(std::string_view function_name);

SQLQueryPiece
applyFunctionHistogramQuantile(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
