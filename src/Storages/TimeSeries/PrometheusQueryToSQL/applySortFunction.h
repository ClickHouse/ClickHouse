#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Returns true if the specified function is sort() or sort_desc().
bool isSortFunction(std::string_view function_name);

/// Applies sort() or sort_desc() to an instant vector.
/// These functions do not change the values — they only affect the output ordering
/// of the final result (ascending for sort(), descending for sort_desc()).
SQLQueryPiece applySortFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);
}
