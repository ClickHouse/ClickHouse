#pragma once

#include <DataTypes/IDataType.h>
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

    /// Assigns proper windows to nodes.
    void setWindows();

    /// Propagates the range of a range selector or a subquery up to its parents until we meet a range-vector function
    /// (e.g. avg_over_time) if any, so such function could user a proper window.
    void propagateRangeToParents(const PQT::Node * node, Decimal64 range);

    std::shared_ptr<const PrometheusQueryTree> promql_tree;
    DataTypePtr timestamp_data_type;
    UInt32 timestamp_scale;
    DurationType instant_selector_window;
    DurationType default_subquery_step;
    std::unordered_map<const Node *, NodeEvaluationRange> map;
};

}
