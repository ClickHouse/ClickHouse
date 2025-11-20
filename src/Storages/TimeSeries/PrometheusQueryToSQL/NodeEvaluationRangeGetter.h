#pragma once

#include <Core/Field.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>


namespace DB
{
    struct PrometheusQueryEvaluationSettings;
}

namespace DB::PrometheusQueryToSQL
{

/// Calculates and keeps evaluation ranges for each node in a PrometheusQueryTree.
class NodeEvaluationRangeGetter
{
public:
    NodeEvaluationRangeGetter(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings);

    /// Returns the evaluation range for a specific node in a PrometheusQueryTree.
    const NodeEvaluationRange & get(const PrometheusQueryTree::Node * node) const;

private:
    void visitNode(
        const PrometheusQueryTree::Node * node,
        const NodeEvaluationRange & range,
        const PrometheusQueryTree & promql_tree,
        const PrometheusQueryEvaluationSettings & settings);

    void visitChildren(
        const PrometheusQueryTree::Node * node,
        const NodeEvaluationRange & range,
        const PrometheusQueryTree & promql_tree,
        const PrometheusQueryEvaluationSettings & settings);

    /// Finds range selectors and sets proper windows for functions taking range vectors.
    void setWindows(const PrometheusQueryEvaluationSettings & settings);

    std::unordered_map<const PrometheusQueryTree::Node *, NodeEvaluationRange> map;
};

}
