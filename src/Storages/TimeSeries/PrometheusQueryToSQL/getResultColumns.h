#pragma once

#include <DataTypes/IDataType_fwd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB
{
    class ColumnsDescription;
}


namespace DB::PrometheusQueryToSQL
{

/// Returns description of the columns returned by function prometheusQuery() or prometheusQueryRange().
ColumnsDescription getResultColumns(
    const PrometheusQueryTree & promql_tree,
    const PrometheusQueryEvaluationSettings & settings,
    const DataTypePtr & value_data_type = nullptr);

}
