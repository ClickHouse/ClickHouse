#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


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
    using TimestampType = DateTime64;
    using DurationType = Decimal64;

    StorageID time_series_storage_id = StorageID::createEmpty();

    /// Data types of the corresponding columns in the TimeSeries table.
    /// We use these data types for the columns we read from table function prometheusQuery().
    DataTypePtr timestamp_data_type;
    DataTypePtr scalar_data_type;

    /// Specifies that the TimeSeries storage has a histograms target, so selectors also read native
    /// histogram samples (see StoreMethod::HISTOGRAM_RAW_DATA).
    bool storage_has_native_histograms = false;

    PrometheusQueryEvaluationMode mode = PrometheusQueryEvaluationMode::QUERY;

    /// Specifies that a prometheus query should be evaluated at the current time.
    bool use_current_time = false;

    /// Evaluation range [`start_time`, `end_time`] with `step`, in the same scale as `timestamp_data_type`.
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
