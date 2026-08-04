#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Returns true if the specified function is `sort` or `sort_desc`.
bool isSortFunction(std::string_view function_name);

/// Applies `sort` or `sort_desc` to an instant vector and captures the order at this point.
SQLQueryPiece applySortFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);
}
