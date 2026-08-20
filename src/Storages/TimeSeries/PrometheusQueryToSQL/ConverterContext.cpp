#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <DataTypes/DataTypesDecimal.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                   const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , timestamp_data_type(settings_.timestamp_data_type)
    , timestamp_scale(tryGetDecimalScale(*timestamp_data_type).value_or(0))
    , scalar_data_type(settings_.scalar_data_type)
    , node_range_getter(promql_tree_, settings_)
    , result_type(getResultType(*promql_tree_, settings_))
{
}

}
