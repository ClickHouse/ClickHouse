#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/alignTimestampWithStep.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// By default the lookback period is 5 minutes.
    constexpr const Int64 DEFAULT_LOOKBACK_SECONDS = 5 * 60;

    /// By default the default resolution is 15 seconds.
    constexpr const Int64 DEFAULT_RESOLUTION_SECONDS = 15;
}

NodeEvaluationRangeGetter::NodeEvaluationRangeGetter(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                                     const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , timestamp_data_type(settings_.timestamp_data_type)
    , timestamp_scale(tryGetDecimalScale(*timestamp_data_type).value_or(0))
{
    if (promql_tree->getTimestampScale() != timestamp_scale)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got two different timestamp scales: {} and {}",
                        promql_tree->getTimestampScale(), timestamp_scale);
    }

    /// By default the lookback period is 5 minutes.
    if (settings_.lookback_delta)
        lookback_delta = *settings_.lookback_delta;
    else
        lookback_delta = DEFAULT_LOOKBACK_SECONDS * DecimalUtils::scaleMultiplier<DurationType>(timestamp_scale);

    /// By default the default resolution is 15 seconds.
    if (settings_.default_resolution)
        default_resolution = *settings_.default_resolution;
    else
        default_resolution = DEFAULT_RESOLUTION_SECONDS * DecimalUtils::scaleMultiplier<DurationType>(timestamp_scale);

    const auto * root = promql_tree->getRoot();
    if (!root)
        return;

    if (settings_.evaluation_range)
    {
        NodeEvaluationRange range{
            .start_time = settings_.evaluation_range->start_time,
            .end_time = settings_.evaluation_range->end_time,
            .step = settings_.evaluation_range->step,
            .window = lookback_delta};
        visitNode(root, range);
    }
    else
    {
        /// By default the evaluation time is the current time.
        TimestampType evaluation_time;
        if (settings_.evaluation_time)
            evaluation_time = *settings_.evaluation_time;
        else
            evaluation_time = DecimalUtils::getCurrentDateTime64(timestamp_scale);

        NodeEvaluationRange range{
            .start_time = evaluation_time,
            .end_time = evaluation_time,
            .step = 0,
            .window = lookback_delta};
        visitNode(root, range);
    }

    findRangeSelectorsAndSetWindows();
}


void NodeEvaluationRangeGetter::visitNode(const Node * node, const NodeEvaluationRange & range)
{
    map[node] = range;
    visitChildren(node, range);
}


void NodeEvaluationRangeGetter::visitChildren(const Node * node, const NodeEvaluationRange & range)
{
    switch (node->node_type)
    {
        case NodeType::Offset:
        {
            const auto * offset_node = static_cast<const PQT::Offset *>(node);
            const auto * expression = offset_node->getExpression();
            NodeEvaluationRange expression_range = range;
            if (auto timestamp = offset_node->at_timestamp)
            {
                expression_range.start_time = *timestamp;
                expression_range.end_time = *timestamp;
            }
            if (auto offset_value = offset_node->offset_value)
            {
                expression_range.start_time -= *offset_value;
                expression_range.end_time -= *offset_value;
            }
            visitNode(expression, expression_range);
            break;
        }

        case NodeType::Subquery:
        {
            const auto * subquery_node = static_cast<const PQT::Subquery *>(node);
            auto subquery_range = subquery_node->range;

            DurationType step;
            if (auto resolution = subquery_node->resolution)
                step = *subquery_node->resolution;
            else
                step = default_resolution;

            const auto * expression = subquery_node->getExpression();
            NodeEvaluationRange expression_range = range;

            /// Subqueries use times that are aligned with the step.
            expression_range.start_time = alignTimestampWithStep(range.start_time - subquery_range + step, step);
            expression_range.end_time = alignTimestampWithStep(range.end_time, step);
            expression_range.step = step;

            visitNode(expression, expression_range);
            break;
        }

        default:
        {
            for (const auto * child : node->children)
                visitNode(child, range);
        }
    }
}


void NodeEvaluationRangeGetter::findRangeSelectorsAndSetWindows()
{
    for (auto it = map.begin(); it != map.end(); ++it)
    {
        const auto * node = it->first;
        if (node->node_type == NodeType::RangeSelector)
        {
            /// We've found a range selector, so we'll propagate its window to a range-vector function
            /// (if this range selector is used in any range-vector function).
            const auto * range_selector_node = static_cast<const PQT::RangeSelector *>(node);
            auto window = range_selector_node->range;
            it->second.window = window;
            const auto * parent = node->parent;
            while (parent)
            {
                map.at(parent).window = window;
                if (parent->result_type != ResultType::RANGE_VECTOR)
                    break;
                parent = parent->parent;
            }
        }
    }
}


const NodeEvaluationRange & NodeEvaluationRangeGetter::get(const Node * node) const
{
    auto it = map.find(node);
    if (it == map.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Not found node {} in NodeEvaluationRangeGetter", promql_tree->getQuery(node));
    }
    return it->second;
}

}
