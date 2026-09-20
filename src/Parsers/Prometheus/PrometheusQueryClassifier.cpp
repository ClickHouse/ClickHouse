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

    auto rate_query = extractPromQLRangeRateQuery(aggregation->getArguments().front(), /* is_query_range = */ true);
    if (!rate_query)
        return {};

    return PromQLRangeSumByQuery{
        .labels_to_keep = aggregation->labels,
        .matchers = std::move(rate_query->matchers),
        .window = rate_query->window,
    };
}

std::optional<PromQLRangeRateQuery> extractRangeRateNode(const Node * root)
{
    const auto * rate = asNode<PrometheusQueryTree::Function>(root, NodeType::Function);
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

    return PromQLRangeRateQuery{
        .matchers = instant_selector->matchers,
        .window = range_selector->range,
        .node = root,
    };
}

bool matcherLess(const PrometheusQueryTree::Matcher & lhs, const PrometheusQueryTree::Matcher & rhs)
{
    if (lhs.label_name != rhs.label_name)
        return lhs.label_name < rhs.label_name;
    if (lhs.matcher_type != rhs.matcher_type)
        return static_cast<int>(lhs.matcher_type) < static_cast<int>(rhs.matcher_type);
    return lhs.label_value < rhs.label_value;
}

bool matcherEqual(const PrometheusQueryTree::Matcher & lhs, const PrometheusQueryTree::Matcher & rhs)
{
    return lhs.label_name == rhs.label_name && lhs.label_value == rhs.label_value && lhs.matcher_type == rhs.matcher_type;
}

bool sameMatcherMultiset(
    const PrometheusQueryTree::MatcherList & lhs,
    const PrometheusQueryTree::MatcherList & rhs)
{
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), matcherEqual);
}

std::optional<PromQLTwoRangeRateQuery> extractTwoRangeRateNode(const Node * root)
{
    const auto * rate = asNode<PrometheusQueryTree::Function>(root, NodeType::Function);
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

    size_t metric_name_matcher_count = 0;
    String metric_name;
    PrometheusQueryTree::MatcherList common_matchers;
    common_matchers.reserve(instant_selector->matchers.size());

    for (const auto & matcher : instant_selector->matchers)
    {
        if (matcher.label_name == "__name__")
        {
            ++metric_name_matcher_count;
            if (matcher.matcher_type != PrometheusQueryTree::MatcherType::EQ || matcher.label_value.empty())
                return {};

            metric_name = matcher.label_value;
        }
        else
        {
            common_matchers.push_back(matcher);
        }
    }

    if (metric_name_matcher_count != 1)
        return {};

    std::sort(common_matchers.begin(), common_matchers.end(), matcherLess);
    return PromQLTwoRangeRateQuery{
        .metric_name = std::move(metric_name),
        .common_matchers = std::move(common_matchers),
        .window = range_selector->range,
        .node = root,
    };
}

std::optional<PromQLTwoRangeRatesQuery> extractTwoRangeRatesNode(const Node * root)
{
    const auto * binary = asNode<PrometheusQueryTree::BinaryOperator>(root, NodeType::BinaryOperator);
    if (!binary || binary->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR || binary->operator_name != "+"
        || binary->children.size() != 2 || binary->on || binary->ignoring || !binary->labels.empty()
        || binary->group_left || binary->group_right || !binary->extra_labels.empty() || binary->bool_modifier)
        return {};

    auto left_rate = extractTwoRangeRateNode(binary->getLeftArgument());
    auto right_rate = extractTwoRangeRateNode(binary->getRightArgument());
    if (!left_rate || !right_rate)
        return {};

    if (left_rate->metric_name == right_rate->metric_name || left_rate->window != right_rate->window
        || !sameMatcherMultiset(left_rate->common_matchers, right_rate->common_matchers))
        return {};

    return PromQLTwoRangeRatesQuery{
        .rates = {std::move(*left_rate), std::move(*right_rate)},
        .binary_node = binary,
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

std::optional<PromQLRangeRateQuery> extractPromQLRangeRateQuery(
    const PrometheusQueryTree::Node * root,
    bool is_query_range)
{
    if (!is_query_range)
        return {};

    return extractRangeRateNode(root);
}

std::optional<PromQLTwoRangeRatesQuery> extractPromQLTwoRangeRatesQuery(
    const PrometheusQueryTree::Node * root,
    bool is_query_range)
{
    if (!is_query_range)
        return {};

    return extractTwoRangeRatesNode(root);
}

std::optional<PromQLTwoRangeRatesSumByQuery> extractPromQLTwoRangeRatesSumByQuery(
    const PrometheusQueryTree & query_tree,
    bool is_query_range)
{
    if (!is_query_range)
        return {};

    const auto * ceil = asNode<PrometheusQueryTree::Function>(query_tree.getRoot(), NodeType::Function);
    if (!ceil || ceil->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR || ceil->function_name != "ceil"
        || ceil->getArguments().size() != 1)
        return {};

    const auto * aggregation
        = asNode<PrometheusQueryTree::AggregationOperator>(ceil->getArguments().front(), NodeType::AggregationOperator);
    if (!aggregation || aggregation->result_type != PrometheusQueryTree::ResultType::INSTANT_VECTOR || aggregation->operator_name != "sum"
        || !aggregation->by || aggregation->without || aggregation->getArguments().size() != 1)
        return {};

    auto two_rates = extractTwoRangeRatesNode(aggregation->getArguments().front());
    if (!two_rates)
        return {};

    return PromQLTwoRangeRatesSumByQuery{
        .labels_to_keep = aggregation->labels,
        .rates = std::move(two_rates->rates),
        .binary_node = two_rates->binary_node,
    };
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
