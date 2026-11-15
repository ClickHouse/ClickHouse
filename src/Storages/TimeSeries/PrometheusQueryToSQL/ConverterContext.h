#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>


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
    const NodeEvaluationRangeGetter node_range_getter;
    const ResultType result_type;
    SQLSubqueries subqueries;

    ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                     const PrometheusQueryEvaluationSettings & settings_);
};

}
