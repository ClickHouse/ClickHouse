#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultSortingByDefault.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(const PrometheusQueryTree & promql_tree_, const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , max_time_scale(getTimeseriesScale(settings_.result_timestamp_type))
    , result_timestamp_type(settings_.result_timestamp_type)
    , limit(settings_.limit)
    , node_evaluation_range_getter(promql_tree_, settings_)
    , result_sorting(getResultSortingByDefault(promql_tree_, settings_))
{
}

}
