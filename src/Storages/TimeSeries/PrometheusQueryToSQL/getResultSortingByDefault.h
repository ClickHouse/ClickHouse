#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{
struct ResultSorting;

/// Returns how the result rows of a specified prometheus query should be sorted by default
/// if functions like sort() or sort_by_label() are not used.
ResultSorting getResultSortingByDefault(const PrometheusQueryTree & promql_tree,
                                        const PrometheusQueryEvaluationSettings & settings);

}
