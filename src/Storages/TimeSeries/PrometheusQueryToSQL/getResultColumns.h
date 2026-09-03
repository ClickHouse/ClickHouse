#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB
{
    class ColumnsDescription;
}


namespace DB::PrometheusQueryToSQL
{

/// Returns the columns of `prometheusQuery`/`prometheusQueryRange`; with `histogram_result` an instant vector gets
/// a Nullable `value` plus a `histogram` column, and a range vector gets an extra `histogram_series` column.
ColumnsDescription getResultColumns(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings, bool histogram_result);

}
