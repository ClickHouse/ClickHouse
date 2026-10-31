#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Peels off Offset wrappers to return the underlying InstantSelector.
/// Returns nullptr if the node is not an instant or range selector.
inline const PrometheusQueryTree::InstantSelector * peelToInstantSelector(const Node * node)
{
    while (node && node->node_type == NodeType::Offset)
        node = static_cast<const PrometheusQueryTree::Offset *>(node)->getExpression();

    if (node && node->node_type == NodeType::RangeSelector)
        return static_cast<const PrometheusQueryTree::RangeSelector *>(node)->getInstantSelector();

    if (node && node->node_type == NodeType::InstantSelector)
        return static_cast<const PrometheusQueryTree::InstantSelector *>(node);

    return nullptr;
}

/// Checks whether the selector contains an equality matcher for __name__.
/// An exact metric name match guarantees all selected series share that metric name.
inline bool hasExactMetricNameMatcher(const Node * node)
{
    const auto * selector = peelToInstantSelector(node);
    if (!selector)
        return false;

    for (const auto & matcher : selector->matchers)
    {
        if (matcher.label_name == kMetricName && matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ)
            return true;
    }
    return false;
}

}
