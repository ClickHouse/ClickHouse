#pragma once

#include <Core/Field.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>


namespace DB::PrometheusQueryToSQL
{

/// Calculates and keeps evaluation ranges for each node in a PQT.
class NodeEvaluationRangeGetter
{
public:
    NodeEvaluationRangeGetter(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings);

    /// Returns the evaluation range for a specific node in a PQT.
    const NodeEvaluationRange & get(const Node * node) const;

private:
    void visitNode(
        const Node * node,
        const NodeEvaluationRange & range,
        const PQT & promql_tree,
        const PrometheusQueryEvaluationSettings & settings);

    void visitChildren(
        const Node * node,
        const NodeEvaluationRange & range,
        const PQT & promql_tree,
        const PrometheusQueryEvaluationSettings & settings);

    /// Finds range selectors and sets proper windows for functions taking range vectors.
    void setWindows(const PrometheusQueryEvaluationSettings & settings);

    std::unordered_map<const Node *, NodeEvaluationRange> map;
};

}
