#include <Parsers/Prometheus/PrometheusQueryClassifier.h>

#include <algorithm>
#include <cmath>
#include <utility>


namespace DB
{

namespace
{
using Node = PrometheusQueryTree::Node;
using NodeType = PrometheusQueryTree::NodeType;

template <typename NodeClass>
const NodeClass * asNode(const Node * node, NodeType expected_type)
{
    if (!node || node->node_type != expected_type)
        return nullptr;

    return typeid_cast<const NodeClass *>(node);
}

std::optional<PromQLRangeSumByQuery> extractRangeSumByNode(const Node * root)
{
    const auto * aggregation = asNode<PrometheusQueryTree::AggregationOperator>(root, NodeType::AggregationOperator);
    if (!aggregation || aggregation->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR || aggregation->operator_name != "sum"
        || !aggregation->by || aggregation->without || aggregation->getArguments().size() != 1)
        return {};

    const auto * rate = asNode<PrometheusQueryTree::Function>(aggregation->getArguments().front(), NodeType::Function);
    if (!rate || rate->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR || rate->function_name != "rate"
        || rate->getArguments().size() != 1)
        return {};

    const auto * range_selector = asNode<PrometheusQueryTree::RangeSelector>(rate->getArguments().front(), NodeType::RangeSelector);
    if (!range_selector || range_selector->result_type != PrometheusQueryTree::ResultType::RANGE_VECTOR || range_selector->range <= 0
        || range_selector->children.size() != 1)
        return {};

    const auto * instant_selector
        = asNode<PrometheusQueryTree::InstantSelector>(range_selector->children.front(), NodeType::InstantSelector);
    if (!instant_selector || instant_selector->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR)
        return {};

    /// `rate` drops the metric name and rejects duplicate output series before the outer aggregation.
    /// The native kernel checks duplicate full tag sets, but after dropping the metric name two different
    /// metrics could still collide. An exact `__name__ = ...` matcher constrains the selector to at most one
    /// metric name and preserves that invariant even if there are additional metric-name matchers.
    if (!std::ranges::any_of(
        instant_selector->matchers,
        [](const auto & matcher)
        { return matcher.label_name == "__name__" && matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ; }))
        return {};

    return PromQLRangeSumByQuery{
        .labels_to_keep = aggregation->labels,
        .matchers = instant_selector->matchers,
        .window = range_selector->range,
    };
}
}

std::optional<PromQLRangeSumByQuery> extractPromQLRangeSumByQuery(
    const PrometheusQueryTree & query_tree,
    bool is_query_range)
{
    return extractPromQLRangeSumByQuery(query_tree.getRoot(), is_query_range);
}

std::optional<PromQLRangeSumByQuery> extractPromQLRangeSumByQuery(
    const PrometheusQueryTree::Node * root,
    bool is_query_range)
{
    if (!is_query_range)
        return {};

    return extractRangeSumByNode(root);
}

std::optional<PromQLRangeTopKByQuery> extractPromQLRangeTopKByQuery(
    const PrometheusQueryTree & query_tree,
    bool is_query_range)
{
    if (!is_query_range)
        return {};

    const auto * aggregation = asNode<PrometheusQueryTree::AggregationOperator>(query_tree.getRoot(), NodeType::AggregationOperator);
    if (!aggregation || aggregation->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR
        || (aggregation->operator_name != "topk" && aggregation->operator_name != "bottomk") || aggregation->by || aggregation->without
        || aggregation->getArguments().size() != 2)
        return {};

    const auto * scalar = asNode<PrometheusQueryTree::Scalar>(aggregation->getArguments().front(), NodeType::Scalar);
    if (!scalar || !std::isfinite(scalar->scalar) || scalar->scalar < 0 || std::floor(scalar->scalar) != scalar->scalar
        || scalar->scalar > PROMQL_NATIVE_TOPK_MAX_K)
        return {};

    auto range_sum = extractRangeSumByNode(aggregation->getArguments().back());
    if (!range_sum)
        return {};

    return PromQLRangeTopKByQuery{
        .range_sum = std::move(*range_sum),
        .k = static_cast<UInt64>(scalar->scalar),
        .bottomk = aggregation->operator_name == "bottomk",
    };
}

bool isSupportedPromQLRangeQuery(const PrometheusQueryTree & query_tree, bool is_query_range)
{
    return extractPromQLRangeSumByQuery(query_tree, is_query_range).has_value()
        || extractPromQLRangeTopKByQuery(query_tree, is_query_range).has_value();
}

}
