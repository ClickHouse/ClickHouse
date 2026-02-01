#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>


namespace DB::PrometheusQueryToSQL
{

/// Calculates and keeps evaluation ranges for each node of a PrometheusQueryTree.
class NodeEvaluationRangeGetter
{
public:
    NodeEvaluationRangeGetter(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                              const PrometheusQueryEvaluationSettings & settings_);

    /// Returns the evaluation range for a specific node of a PrometheusQueryTree.
    const NodeEvaluationRange & get(const Node * node) const;

private:
    void visitNode(const Node * node, const NodeEvaluationRange & range);
    void visitChildren(const Node * node, const NodeEvaluationRange & range);

    /// Finds range selectors and sets proper windows for functions taking range vectors.
    void findRangeSelectorsAndSetWindows();

    std::shared_ptr<const PrometheusQueryTree> promql_tree;
    DataTypePtr timestamp_data_type;
    UInt32 timestamp_scale;
    DurationType lookback_delta;
    DurationType default_resolution;
    std::unordered_map<const Node *, NodeEvaluationRange> map;
};

}
