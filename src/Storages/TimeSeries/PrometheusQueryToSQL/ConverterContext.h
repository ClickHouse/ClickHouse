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

    /// Data type of the timestamp column in the TimeSeries table.
    DataTypePtr table_timestamp_type;
    UInt32 table_timestamp_scale;

    /// Data types of the columns `timestamp` and `value` returned by the query built by function finalizeSQL().
    /// All timestamps and durations in the converter (see TimestampType and DurationType) use `result_timestamp_scale`.
    DataTypePtr result_timestamp_type;
    UInt32 result_timestamp_scale;
    DataTypePtr result_value_type;

    /// The version of the TimeSeries table.
    UInt64 time_series_version = TimeSeriesVersion::LATEST;

    const NodeEvaluationRangeGetter node_range_getter;
    const ResultType result_type;
    SQLSubqueries subqueries;

    ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                     const PrometheusQueryEvaluationSettings & settings_);
};

}
