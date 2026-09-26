#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/getPromQLResultTimestampType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                   const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , cluster_name(settings_.cluster_name)
    , remote_time_series_storage_id(settings_.remote_time_series_storage_id)
    , skip_unavailable_shards(settings_.skip_unavailable_shards)
    , skip_unavailable_shards_mode(settings_.skip_unavailable_shards_mode)
    , time_series_version(settings_.time_series_version)
    , result_timestamp_type(getPromQLResultTimestampType(settings_.time_scale, settings_.time_zone))
    , result_timestamp_scale(settings_.time_scale)
    , result_type(getResultType(*promql_tree_, settings_))
    , node_range_getter(promql_tree_, settings_)
{
    /// The result scale is the scale of the table but not less than 3, see getPromQLResultTimestampScale().
    const UInt32 min_result_timestamp_scale = getPromQLResultTimestampScale(settings_.table_timestamp_type);
    if (result_timestamp_scale < min_result_timestamp_scale)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The timestamps in the result of the query have scale {} which is less than the minimum scale {}",
                        result_timestamp_scale, min_result_timestamp_scale);
    }
}

}
