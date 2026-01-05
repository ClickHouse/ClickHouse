#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultSortingByDefault.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                   const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , node_evaluation_range_getter(promql_tree_, settings_)
    , time_scale(getResultTimeScale(settings_))
    , result_type(getResultType(*promql_tree_, settings_))
    , result_time_type(getResultTimeType(settings_))
    , result_scalar_type(getResultScalarType(settings_))
    , limit(settings_.limit)
    , result_sorting(getResultSortingByDefault(*promql_tree_, settings_))
{
}

}
