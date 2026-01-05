#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if a prometheus query allows evaluating over a range and throws an exception if not.
    /// If a query normally returns a scalar or an instant vector then it can be evaluated over a range
    /// and produce a range vector as a result.
    void checkPrometheusQueryAllowsEvaluationRange(const PQT & promql_tree)
    {
        if ((promql_tree.getResultType() == ResultType::SCALAR)
            || (promql_tree.getResultType() == ResultType::INSTANT_VECTOR))
            return;

        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Invalid expression type {} for range query, must be {} or {}",
                        promql_tree.getResultType(),
                        ResultType::SCALAR,
                        ResultType::INSTANT_VECTOR);
    }

    /// A prometheus query can specify durations in milliseconds,
    /// so for the "time" data type we need scale to be at least 3.
    constexpr UInt32 MIN_RESULT_TIME_SCALE = 3;
}


ResultType getResultType(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    if (settings.evaluation_time)
    {
        return promql_tree.getResultType();
    }
    else if (settings.evaluation_range)
    {
        checkPrometheusQueryAllowsEvaluationRange(promql_tree);
        return ResultType::RANGE_VECTOR;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Either evaluation time or evaluation range should be set");
    }
}

DataTypePtr getResultTimeType(const PrometheusQueryEvaluationSettings & settings)
{
    auto timestamp_type = settings.data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    UInt32 time_scale = std::max(getTimeseriesTimeScale(timestamp_type), MIN_RESULT_TIME_SCALE);
    String timezone = getTimeseriesTimezone(timestamp_type);
    return makeTimeseriesTimeDataType(time_scale, timezone);
}

UInt32 getResultTimeScale(const PrometheusQueryEvaluationSettings & settings)
{
    return getResultTimeScale(settings.data_table_metadata);
}

UInt32 getResultTimeScale(const StorageMetadataPtr & data_table_metadata)
{
    auto timestamp_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    return std::max(getTimeseriesTimeScale(timestamp_type), MIN_RESULT_TIME_SCALE);
}

DataTypePtr getResultScalarType(const PrometheusQueryEvaluationSettings &)
{
    return std::make_shared<DataTypeFloat64>();
}

}
