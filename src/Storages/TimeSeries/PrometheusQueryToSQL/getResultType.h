#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB
{
    class ColumnsDescription;
}


namespace DB::PrometheusQueryToSQL
{

/// Returns description of the columns returned by the query built by function finalizeSQL().
ResultType getResultType(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings);

/// Returns description of the columns returned by function prometheusQuery() or prometheusQueryRange().
ColumnsDescription getResultColumns(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings);

}
