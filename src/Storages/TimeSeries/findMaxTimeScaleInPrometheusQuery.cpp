#include <Storages/TimeSeries/findMaxTimeScaleInPrometheusQuery.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace
{
    void findMaxTimeScaleImpl(const PrometheusQueryTree::Node * node, UInt32 & scale)
    {
        if (node->node_type == PrometheusQueryTree::NodeType::IntervalLiteral)
        {
            const auto * interval_node = static_cast<const PrometheusQueryTree::IntervalLiteral *>(node);
            scale = std::max(scale, interval_node->interval.getScale());
        }
        for (const auto * child : node->children)
            findMaxTimeScaleImpl(child, scale);
    }
}


UInt32 findMaxTimeScaleInPrometheusQuery(const PrometheusQueryTree & promql_tree)
{
    UInt32 scale = 0;
    if (const auto * root = promql_tree.getRoot())
        findMaxTimeScaleImpl(root, scale);
    return scale;
}

}
