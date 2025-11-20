#pragma once


namespace DB
{
    class ColumnsDescription;
    enum class PrometheusQueryResultType;
    class PrometheusQueryTree;
    struct PrometheusQueryEvaluationSettings;
}


namespace DB::PrometheusQueryToSQL
{

/// Returns description of the columns returned by the query built by function finalizeSQL().
PrometheusQueryResultType getResultType(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings);

/// Returns description of the columns returned by function prometheusQuery() or prometheusQueryRange().
ColumnsDescription getResultColumns(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings);

}
