#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>


namespace DB::PrometheusQueryToSQL
{

/// Contains information which is used to convert a prometheus query to SQL.
struct ConverterContext
{
    const std::shared_ptr<const PrometheusQueryTree> promql_tree;

    const StorageID time_series_storage_id;
    UInt64 time_series_version = TimeSeriesVersion::LATEST;

    /// Data type of the column `timestamp` returned by the query built by the converter.
    /// All timestamps and durations in the converter (see TimestampType and DurationType) use `result_timestamp_scale`.
    /// The samples read from the TimeSeries table keep the types of the table, see the comment for StoreMethod::RAW_DATA.
    DataTypePtr result_timestamp_type;
    UInt32 result_timestamp_scale;

    const bool use_quantile_prometheus_histogram_array;

    const ResultType result_type;
    const NodeEvaluationRangeGetter node_range_getter;
    SQLSubqueries subqueries;

    ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                     const PrometheusQueryEvaluationSettings & settings_);
};

}
