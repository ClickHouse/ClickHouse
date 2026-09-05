#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of the PromQL function `absent`.
inline bool isFunctionAbsent(std::string_view function_name)
{
    return function_name == "absent";
}

/// Applies the PromQL function `absent`.
SQLQueryPiece applyFunctionAbsent(const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
