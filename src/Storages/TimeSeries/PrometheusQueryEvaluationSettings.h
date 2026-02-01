#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>


namespace DB
{

struct PrometheusQueryEvaluationSettings
{
    using TimestampType = PrometheusQueryEvaluationRange::TimestampType;
    using DurationType = PrometheusQueryEvaluationRange::DurationType;

    StorageID time_series_storage_id = StorageID::createEmpty();
    DataTypePtr timestamp_data_type;
    DataTypePtr scalar_data_type;

    /// `evaluation_time` sets a specific time when the prometheus query is evaluated,
    /// `evaluation_range` sets a range of such times.
    /// If neither `evaluation_time` nor `evaluation_range` is set then the current time is used.
    /// The scale for these fields is the same as the scale used in `timestamp_data_type`.
    std::optional<TimestampType> evaluation_time;
    std::optional<PrometheusQueryEvaluationRange> evaluation_range;

    /// The lookback period. If not set then 5 minutes are used by default.
    std::optional<DurationType> lookback_delta;

    /// The default subquery resolution. If not set then 15 seconds are used by default.
    std::optional<DurationType> default_resolution;
};

}
