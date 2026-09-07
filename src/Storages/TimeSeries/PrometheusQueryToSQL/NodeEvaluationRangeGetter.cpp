#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/alignTimestampWithStep.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// By default the lookback period is 5 minutes.
    constexpr const Int64 DEFAULT_INSTANT_SELECTOR_WINDOW_SECONDS = 5 * 60;

    /// The default subquery step is 15 seconds.
    constexpr const Int64 DEFAULT_SUBQUERY_STEP_SECONDS = 15;
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
    if (settings_.instant_selector_window)
        instant_selector_window = *settings_.instant_selector_window;
    else
        instant_selector_window = DEFAULT_INSTANT_SELECTOR_WINDOW_SECONDS * DecimalUtils::scaleMultiplier<DurationType>(timestamp_scale);

    /// The default subquery step is 15 seconds.
    if (settings_.default_subquery_step)
        default_subquery_step = *settings_.default_subquery_step;
    else
        default_subquery_step = DEFAULT_SUBQUERY_STEP_SECONDS * DecimalUtils::scaleMultiplier<DurationType>(timestamp_scale);

    const auto * root = promql_tree->getRoot();
    if (!root)
        return;

    NodeEvaluationRange range;

    if (settings_.use_current_time)
    {
        range.start_time = DecimalUtils::getCurrentDateTime64(timestamp_scale);
        range.end_time = range.start_time;
        range.step = 0;
    }
    else
    {
        if (!settings_.start_time)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "start_time is not specified");
        if (!settings_.end_time)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "end_time is not specified");
        if (*settings_.start_time > *settings_.end_time)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "start_time must not be greater than end_time");
        if (*settings_.start_time < *settings_.end_time)
        {
            if (!settings_.step)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "step is not specified");
            if (*settings_.step <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "step must be positive");
        }
        range.start_time = *settings_.start_time;
        range.end_time = *settings_.end_time;
        range.step = (*settings_.start_time < *settings_.end_time) ? *settings_.step : DurationType{0};
    }

    visitNode(root, range);
    setWindows();
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
            if (auto subquery_step = subquery_node->step)
                step = *subquery_step;
            else
                step = default_subquery_step;

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


void NodeEvaluationRangeGetter::setWindows()
{
    /// Assign windows for instant selectors.
    for (auto & [node, node_range] : map)
    {
        if (node->node_type == NodeType::InstantSelector)
        {
            /// The following setting may be overwritten later if this instant selector node is a part of a range selector
            /// (see below).
            node_range.window = instant_selector_window;
        }
        else
        {
            /// If the window isn't used we set it to zero.
            node_range.window = 0;
        }
    }

    /// Assign windows for range selectors and subqueries, and propagate these ranges to other nodes.
    for (auto & [node, node_range] : map)
    {
        if (node->node_type == NodeType::RangeSelector)
        {
            const auto * range_selector_node = static_cast<const PQT::RangeSelector *>(node);
            auto range = range_selector_node->range;
            node_range.window = range;
            const auto * instant_selector_node = range_selector_node->getInstantSelector();
            map.at(instant_selector_node).window = range;
            /// We propagate the range of a range selector up to its parents until we meet a range-vector function
            /// (e.g. avg_over_time) if any, so such function could user a proper window.
            propagateRangeToParents(node, range);
        }
        else if (node->node_type == NodeType::Subquery)
        {
            /// We propagate the range of a subquery up to its parents until we meet a range-vector function
            /// (e.g. avg_over_time) if any, so such function could user a proper window.
            const auto * subquery_node = static_cast<const PQT::Subquery *>(node);
            auto range = subquery_node->range;
            node_range.window = range;
            propagateRangeToParents(node, range);
        }
    }
}


void NodeEvaluationRangeGetter::propagateRangeToParents(const PQT::Node * node, Decimal64 range)
{
    chassert(node->result_type == ResultType::RANGE_VECTOR);
    const auto * parent = node->parent;
    while (parent)
    {
        map.at(parent).window = range;
        if (parent->result_type != ResultType::RANGE_VECTOR)
            break;
        parent = parent->parent;
    }
}


const NodeEvaluationRange & NodeEvaluationRangeGetter::get(const Node * node) const
{
    auto it = map.find(node);
    if (it == map.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found node {} in NodeEvaluationRangeGetter", node->node_type);
    }
    return it->second;
}

}
