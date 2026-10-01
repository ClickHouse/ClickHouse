#include <Storages/TimeSeries/PrometheusQueryToSQL/pushDownLabelMatchers.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorCountValues.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorQuantile.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorAnd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorOr.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorUnless.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyComparisonOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionPredictLinear.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionQuantileOverTime.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentAggregationOperator.h>
#include <algorithm>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    using Matcher = PrometheusQueryTree::Matcher;
    using MatcherList = PrometheusQueryTree::MatcherList;
    using BinaryOperatorNode = PrometheusQueryTree::BinaryOperator;
    using AggregationOperatorNode = PrometheusQueryTree::AggregationOperator;

    /// The work of the pass grows fast with the number of binary operators, so bigger expressions are left as they are.
    constexpr size_t MAX_BINARY_OPERATORS = 20;

    bool contains(const Strings & labels, const String & label)
    {
        return std::find(labels.begin(), labels.end(), label) != labels.end();
    }

    /// Keeps the matchers on the labels which the binary operator matches series by.
    MatcherList keepMatchingLabels(MatcherList matchers, const BinaryOperatorNode & binary_operator)
    {
        std::erase_if(matchers, [&](const Matcher & matcher)
        {
            return contains(binary_operator.labels, matcher.label_name) != binary_operator.on;
        });
        return matchers;
    }

    /// Keeps the matchers on the labels which the aggregation keeps in its result.
    MatcherList keepGroupingLabels(MatcherList matchers, const AggregationOperatorNode & aggregation)
    {
        if (!aggregation.by && !aggregation.without)
            return {};
        std::erase_if(matchers, [&](const Matcher & matcher)
        {
            return contains(aggregation.labels, matcher.label_name) != aggregation.by;
        });
        return matchers;
    }

    /// Appends the matchers which `dest` doesn't have yet.
    void appendMissingMatchers(MatcherList & dest, const MatcherList & matchers)
    {
        for (const auto & matcher : matchers)
        {
            bool exists = std::ranges::any_of(dest, [&](const Matcher & other)
            {
                return other.label_name == matcher.label_name && other.label_value == matcher.label_value
                    && other.matcher_type == matcher.matcher_type;
            });
            if (!exists)
                dest.push_back(matcher);
        }
    }

    /// Whether the node is a binary operator between two instant vectors other than `or`.
    bool isVectorMatching(const Node * node)
    {
        return node->node_type == NodeType::BinaryOperator
            && !isBinaryOperatorOr(static_cast<const BinaryOperatorNode *>(node)->operator_name)
            && node->children.at(0)->result_type == ResultType::INSTANT_VECTOR
            && node->children.at(1)->result_type == ResultType::INSTANT_VECTOR;
    }

    /// Returns the argument whose series become the node's result series with the same labels, or nullptr.
    const Node * getLabelPreservingArgument(const Node * node)
    {
        switch (node->node_type)
        {
            case NodeType::RangeSelector:
            case NodeType::Subquery:
            case NodeType::Offset:
                return node->children.at(0);

            case NodeType::Function:
            {
                const auto & function_name = static_cast<const PrometheusQueryTree::Function *>(node)->function_name;
                bool is_function_over_range = isFunctionOverRange(function_name) || isFunctionQuantileOverTime(function_name)
                    || isFunctionPredictLinear(function_name);
                if (!is_function_over_range)
                    return nullptr;
                for (const auto * argument : node->children)
                {
                    if (argument->result_type == ResultType::RANGE_VECTOR)
                        return argument;
                }
                return nullptr;
            }

            case NodeType::BinaryOperator:
            {
                /// An operator between a vector and a scalar keeps the labels of the vector.
                const auto * left = node->children.at(0);
                const auto * right = node->children.at(1);
                if (left->result_type == ResultType::SCALAR && right->result_type == ResultType::INSTANT_VECTOR)
                    return right;
                if (left->result_type == ResultType::INSTANT_VECTOR && right->result_type == ResultType::SCALAR)
                    return left;
                return nullptr;
            }

            default:
                return nullptr;
        }
    }

    /// Whether the series of the node come from one metric, so they stay different after the metric name is dropped.
    bool hasSingleMetricName(const Node * node)
    {
        if (node->node_type == NodeType::InstantSelector)
        {
            const auto & matchers = static_cast<const PrometheusQueryTree::InstantSelector *>(node)->matchers;
            return std::ranges::any_of(matchers, [](const Matcher & matcher)
            {
                return matcher.label_name == kMetricName && matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ;
            });
        }
        const auto * argument = getLabelPreservingArgument(node);
        return argument && hasSingleMetricName(argument);
    }

    /// Whether filtering this side of the operator can't hide an error Prometheus gives for duplicate series.
    /// Prometheus checks the "one" side for duplicates in every match group, so it's filtered only if it can't have any.
    bool canFilterSide(const Node * side, const BinaryOperatorNode & binary_operator)
    {
        const auto & operator_name = binary_operator.operator_name;
        if (isBinaryOperatorAnd(operator_name) || isBinaryOperatorUnless(operator_name))
            return true;
        const auto * one_side = binary_operator.group_right ? binary_operator.getLeftArgument() : binary_operator.getRightArgument();
        if (side != one_side)
            return true;

        /// An aggregation by labels which are all matched on has one series in each match group.
        if (side->node_type != NodeType::AggregationOperator)
            return false;
        const auto & aggregation = static_cast<const AggregationOperatorNode &>(*side);
        if (!aggregation.by
            || !(isOneArgumentAggregationOperator(aggregation.operator_name) || isAggregationOperatorQuantile(aggregation.operator_name)))
            return false;
        return std::ranges::all_of(aggregation.labels, [&](const String & label)
        {
            return label != kMetricName && contains(binary_operator.labels, label) == binary_operator.on;
        });
    }

    /// Returns the matchers which every series of the node's result satisfies, except the ones on the metric name.
    MatcherList getCommonMatchers(const Node * node)
    {
        if (node->node_type == NodeType::InstantSelector)
        {
            MatcherList matchers = static_cast<const PrometheusQueryTree::InstantSelector *>(node)->matchers;
            std::erase_if(matchers, [](const Matcher & matcher) { return matcher.label_name == kMetricName; });
            return matchers;
        }

        if (node->node_type == NodeType::AggregationOperator)
        {
            const auto & aggregation = static_cast<const AggregationOperatorNode &>(*node);
            if (isAggregationOperatorCountValues(aggregation.operator_name))
                return {};
            return keepGroupingLabels(getCommonMatchers(aggregation.children.back()), aggregation);
        }

        if (isVectorMatching(node))
        {
            /// The result of `unless` has only left series, so only the matchers of the left side hold for it.
            const auto & binary_operator = static_cast<const BinaryOperatorNode &>(*node);
            MatcherList matchers = getCommonMatchers(binary_operator.getLeftArgument());
            if (!isBinaryOperatorUnless(binary_operator.operator_name))
                appendMissingMatchers(matchers, getCommonMatchers(binary_operator.getRightArgument()));
            return keepMatchingLabels(std::move(matchers), binary_operator);
        }

        if (const auto * argument = getLabelPreservingArgument(node))
            return getCommonMatchers(argument);

        return {};
    }

    /// Adds the matchers to the selectors of the node, so it returns only the result series which satisfy them.
    void addMatchers(const Node * node, const MatcherList & matchers)
    {
        if (matchers.empty())
            return;

        if (node->node_type == NodeType::InstantSelector)
        {
            /// The nodes belong to a copy of the tree made by pushDownLabelMatchers(), so they can be changed.
            const auto & selector = static_cast<const PrometheusQueryTree::InstantSelector &>(*node);
            appendMissingMatchers(const_cast<MatcherList &>(selector.matchers), matchers);
            return;
        }

        if (node->node_type == NodeType::AggregationOperator)
        {
            const auto & aggregation = static_cast<const AggregationOperatorNode &>(*node);
            if (!isAggregationOperatorCountValues(aggregation.operator_name))
                addMatchers(aggregation.children.back(), keepGroupingLabels(matchers, aggregation));
            return;
        }

        if (isVectorMatching(node))
        {
            /// Other operators fail on duplicate series in a match group, so removing their groups could hide that error.
            const auto & binary_operator = static_cast<const BinaryOperatorNode &>(*node);
            if (!isBinaryOperatorAnd(binary_operator.operator_name) && !isBinaryOperatorUnless(binary_operator.operator_name))
                return;
            MatcherList matching = keepMatchingLabels(matchers, binary_operator);
            for (const auto * side : binary_operator.children)
                addMatchers(side, matching);
            return;
        }

        if (const auto * argument = getLabelPreservingArgument(node))
        {
            /// Functions and operators drop the metric name, except a comparison without `bool`,
            /// and Prometheus fails if two series become the same then.
            bool can_drop_metric_name = (node->node_type == NodeType::Function);
            if (node->node_type == NodeType::BinaryOperator)
            {
                const auto & binary_operator = static_cast<const BinaryOperatorNode &>(*node);
                can_drop_metric_name = !isComparisonOperator(binary_operator.operator_name) || binary_operator.bool_modifier;
            }
            if (!can_drop_metric_name || hasSingleMetricName(argument))
                addMatchers(argument, matchers);
        }
    }

    size_t countBinaryOperators(const Node * node)
    {
        size_t count = (node->node_type == NodeType::BinaryOperator) ? 1 : 0;
        for (const auto * child : node->children)
            count += countBinaryOperators(child);
        return count;
    }

    void pushDownAcrossBinaryOperators(const Node * node)
    {
        for (const auto * child : node->children)
            pushDownAcrossBinaryOperators(child);

        if (!isVectorMatching(node))
            return;

        /// A series without a match on the other side is dropped, except the left series of `unless`, which are kept.
        const auto & binary_operator = static_cast<const BinaryOperatorNode &>(*node);
        MatcherList left_matchers = keepMatchingLabels(getCommonMatchers(binary_operator.getLeftArgument()), binary_operator);
        MatcherList right_matchers = keepMatchingLabels(getCommonMatchers(binary_operator.getRightArgument()), binary_operator);

        if (canFilterSide(binary_operator.getRightArgument(), binary_operator))
            addMatchers(binary_operator.getRightArgument(), left_matchers);
        if (!isBinaryOperatorUnless(binary_operator.operator_name) && canFilterSide(binary_operator.getLeftArgument(), binary_operator))
            addMatchers(binary_operator.getLeftArgument(), right_matchers);
    }
}


std::shared_ptr<const PrometheusQueryTree> pushDownLabelMatchers(std::shared_ptr<const PrometheusQueryTree> promql_tree)
{
    const auto * root = promql_tree->getRoot();
    if (!root || countBinaryOperators(root) > MAX_BINARY_OPERATORS)
        return promql_tree;
    auto res = std::make_shared<PrometheusQueryTree>(*promql_tree);
    pushDownAcrossBinaryOperators(res->getRoot());
    return res;
}

}
