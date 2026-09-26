#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>


namespace DB
{

enum class PrometheusQueryEvaluationMode
{
    /// Evaluates at a single time (`start_time` = `end_time`, or `use_current_time`); corresponds to endpoint /api/v1/query.
    QUERY,

    /// Evaluates a query over a range of time at a specified evaluation time set by `start_time` and `end_time` (they must be equal).
    /// Corresponds to endpoint /api/v1/query_range
    QUERY_RANGE,
};


struct PrometheusQueryEvaluationSettings
{
    StorageID time_series_storage_id = StorageID::createEmpty();
    UInt64 time_series_version = TimeSeriesVersion::LATEST;

    /// Specifies that the TimeSeries storage has a histograms target, so selectors also read native
    /// histogram samples (see StoreMethod::HISTOGRAM_RAW_DATA).
    bool storage_has_native_histograms = false;

    /// Data type of the timestamp column in the TimeSeries table.
    DataTypePtr table_timestamp_type;

    PrometheusQueryEvaluationMode mode = PrometheusQueryEvaluationMode::QUERY;

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

    /// The lookback window of instant selectors: `http_requests_total @ 1770810669` is evaluated as
    /// `last_over_time(http_requests_total[<instant_selector_window>] @ 1770810669)`; 5 minutes by default.
    std::optional<DurationType> instant_selector_window;

    /// The step for subqueries without an explicit one, as in `http_requests_total[10m:]`; 15 seconds by default.
    std::optional<DurationType> default_subquery_step;
};

}
