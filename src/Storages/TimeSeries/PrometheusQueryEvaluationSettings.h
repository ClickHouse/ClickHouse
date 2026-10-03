#pragma once

#include <Common/ClickHouseVersion.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>

#include <string_view>


namespace DB
{

enum class PrometheusQueryEvaluationMode
{
    /// Evaluates a query at a specified evaluation time set either by `start_time` and `end_time` (they must be equal),
    /// or by turning on `use_current_time`.
    /// Corresponds to endpoint /api/v1/query
    QUERY,

    /// Evaluates a query over a range of time at a specified evaluation time set by `start_time` and `end_time` (they must be equal).
    /// Corresponds to endpoint /api/v1/query_range
    QUERY_RANGE,
};


/// The packed aggregate was introduced in 26.10 and is not available on older servers.
inline bool useQuantilePrometheusHistogramArray(std::string_view compatibility)
{
    static const ClickHouseVersion minimum_version{"26.10"};
    return compatibility.empty() || ClickHouseVersion(compatibility) >= minimum_version;
}


struct PrometheusQueryEvaluationSettings
{
    StorageID time_series_storage_id = StorageID::createEmpty();
    UInt64 time_series_version = TimeSeriesVersion::LATEST;

    /// Data type of the timestamp column in the TimeSeries table.
    DataTypePtr table_timestamp_type;

    PrometheusQueryEvaluationMode mode = PrometheusQueryEvaluationMode::QUERY;

    /// Keep the old SQL lowering for compatibility with servers before 26.10.
    bool use_quantile_prometheus_histogram_array = true;

    /// Specifies that a prometheus query should be evaluated at the current time.
    bool use_current_time = false;

    /// Scale of all timestamps and durations in these settings (`start_time`, `end_time`, `step`, `instant_selector_window`,
    /// `default_subquery_step`) and of the parsed PromQL query.
    /// Should be assigned by calling function getPromQLResultTimestampScale
    /// because it is also the scale of the timestamps in the query result.
    UInt32 time_scale = 3;

    /// Time zone of the timestamps in the results of the query.
    /// Empty means the server's time zone.
    String time_zone;

    using TimestampType = DateTime64;
    using DurationType = Decimal64;

    /// Specifies that a prometheus query should be evaluated starting with `start_time` and ending with `end_time`
    /// with a specified `step`.
    /// The scale of these fields is `time_scale`.
    std::optional<TimestampType> start_time;
    std::optional<TimestampType> end_time;
    std::optional<DurationType> step;

    /// The window used by instant selectors (see lookback period).
    /// For example, query "http_requests_total @ 1770810669" is in fact evaluated as
    /// "last_over_time(http_requests_total[<instant_selector_window>] @ 1770810669)"
    /// If not set then it's 5 minutes by default.
    std::optional<DurationType> instant_selector_window;

    /// The default subquery step is used for subqueries specified without explicit step,
    /// for example "http_requests_total[10m:]"
    /// (If a step is given in the subquery, as in "http_requests_total[10m:1m]", then the given step is used.)
    /// If not set then it's 15 seconds by default.
    std::optional<DurationType> default_subquery_step;
};

}
