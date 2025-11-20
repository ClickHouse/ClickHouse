#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ResultSorting.h>


namespace DB
{
    class PrometheusQueryTree;
}


namespace DB::PrometheusQueryToSQL
{

struct ConverterConfig;

/// Contains information used for converting prometheus query to SQL query.
struct ConverterContext
{
    const PrometheusQueryTree & promql_tree;
    const StorageID time_series_storage_id;
    const UInt32 max_time_scale;
    const DataTypePtr result_timestamp_type;
    const std::optional<size_t> limit;
    const NodeEvaluationRangeGetter node_evaluation_range_getter;
    ResultSorting result_sorting;
    SQLSubqueries subqueries;

    ConverterContext(const PrometheusQueryTree & promql_tree_, const PrometheusQueryEvaluationSettings & settings_);
};

}
