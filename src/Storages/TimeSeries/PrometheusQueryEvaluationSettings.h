#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


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


struct PrometheusQueryEvaluationSettings
{
    using TimestampType = DateTime64;
    using DurationType = Decimal64;

    StorageID time_series_storage_id = StorageID::createEmpty();

    /// Data types of the corresponding columns in the TimeSeries table.
    /// We use these data types for the columns we read from table function prometheusQuery().
    DataTypePtr timestamp_data_type;
    DataTypePtr scalar_data_type;

    PrometheusQueryEvaluationMode mode = PrometheusQueryEvaluationMode::QUERY;

    /// Specifies that a prometheus query should be evaluated at the current time.
    bool use_current_time = false;

    /// Specifies that a prometheus query should be evaluated starting with `start_time` and ending with `end_time`
    /// with a specified `step`.
    /// The scale of these fields is the same as the scale used in `timestamp_data_type`.
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
