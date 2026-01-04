#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ResultSorting.h>


namespace DB::PrometheusQueryToSQL
{

/// Contains information used for converting prometheus query to SQL query.
struct ConverterContext
{
    const std::shared_ptr<const PrometheusQueryTree> promql_tree;
    const StorageID time_series_storage_id;
    const NodeEvaluationRangeGetter node_evaluation_range_getter;
    const UInt32 time_scale;
    const ResultType result_type;
    const DataTypePtr result_time_type;
    const DataTypePtr result_scalar_type;
    const std::optional<size_t> limit;
    ResultSorting result_sorting;
    SQLSubqueries subqueries;

    ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                     const PrometheusQueryEvaluationSettings & settings_);
};

}
