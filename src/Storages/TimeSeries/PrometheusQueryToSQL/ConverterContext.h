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
    DataTypePtr timestamp_data_type;
    UInt32 timestamp_scale;
    DataTypePtr scalar_data_type;

    /// The version of the TimeSeries table.
    UInt64 time_series_version = TimeSeriesVersion::LATEST;

    /// True if the TimeSeries storage has a histograms target (see PrometheusQueryEvaluationSettings::storage_has_native_histograms).
    const bool storage_has_native_histograms;

    const NodeEvaluationRangeGetter node_range_getter;
    const ResultType result_type;
    SQLSubqueries subqueries;

    ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                     const PrometheusQueryEvaluationSettings & settings_);
};

}
