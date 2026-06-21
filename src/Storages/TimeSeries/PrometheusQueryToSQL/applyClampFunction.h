#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

bool isClampFunction(std::string_view function_name);

SQLQueryPiece applyClampFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
