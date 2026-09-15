#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <DataTypes/DataTypesDecimal.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
#include <Storages/TimeSeries/getPromQLResultTypes.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                   const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , table_timestamp_type(settings_.table_timestamp_type)
    , table_timestamp_scale(tryGetDecimalScale(*table_timestamp_type).value_or(0))
    , result_timestamp_type(getPromQLResultTimestampType(table_timestamp_type))
    , result_timestamp_scale(getPromQLResultTimestampScale(table_timestamp_type))
    , result_value_type(getPromQLResultValueType())
    , time_series_version(settings_.time_series_version)
    , node_range_getter(promql_tree_, settings_)
    , result_type(getResultType(*promql_tree_, settings_))
{
}

}
