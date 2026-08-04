#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Returns true if the specified function is sort(), sort_desc(), sort_by_label() or sort_by_label_desc().
bool isSortFunction(std::string_view function_name);

/// Applies sort(), sort_desc(), sort_by_label() or sort_by_label_desc() to an instant vector.
/// These functions do not change the values — they only affect the output ordering of the final result:
/// sort()/sort_desc() order by sample value (ascending/descending); sort_by_label()/sort_by_label_desc()
/// order by the natural sort order of the given label values (ascending/descending).
SQLQueryPiece applySortFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);
}
