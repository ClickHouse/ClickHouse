#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns description of the columns returned by the query built by function finalizeSQL().
ResultType getResultType(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings);

}
