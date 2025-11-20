#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTime.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

NodeEvaluationRangeGetter::NodeEvaluationRangeGetter(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    const auto * root = promql_tree.getRoot();
    if (root)
    {
        if (settings.evaluation_time)
        {
            NodeEvaluationRange range{
                .start_time = *settings.evaluation_time,
                .end_time = *settings.evaluation_time,
                .step = DecimalField<Decimal64>{},
                .window = settings.lookback_delta};
            visitNode(root, range, promql_tree, settings);
        }
        else if (settings.evaluation_range)
        {
            NodeEvaluationRange range{
                .start_time = settings.evaluation_range->start_time,
                .end_time = settings.evaluation_range->end_time,
                .step = settings.evaluation_range->step,
                .window = settings.lookback_delta};
            visitNode(root, range, promql_tree, settings);
        }
    }
    setWindows(settings);
}


void NodeEvaluationRangeGetter::visitNode(
    const PrometheusQueryTree::Node * node,
    const NodeEvaluationRange & range,
    const PrometheusQueryTree & promql_tree,
    const PrometheusQueryEvaluationSettings & settings)
{
    map[node] = range;
    visitChildren(node, range, promql_tree, settings);
}


void NodeEvaluationRangeGetter::visitChildren(
    const PrometheusQueryTree::Node * node,
    const NodeEvaluationRange & range,
    const PrometheusQueryTree & promql_tree,
    const PrometheusQueryEvaluationSettings & settings)
{
    switch (node->node_type)
    {
        case PrometheusQueryTree::NodeType::At:
        {
            const auto * at_node = static_cast<const PrometheusQueryTree::At *>(node);
            const auto * expression = at_node->getExpression();
            NodeEvaluationRange expression_range = range;
            if (const auto * at = at_node->getAt())
            {
                auto max_scale = getTimeseriesScale(settings.result_timestamp_type);
                auto timestamp = nodeToTime(at, max_scale);
                if (const auto * offset = at_node->getOffset())
                {
                    auto offset_value = nodeToDuration(offset, max_scale);
                    timestamp = subtractTimeseriesDuration(timestamp, offset_value);
                }
                expression_range.start_time = timestamp;
                expression_range.end_time = timestamp;
            }
            else if (const auto * offset = at_node->getOffset())
            {
                auto max_scale = getTimeseriesScale(settings.result_timestamp_type);
                auto offset_value = nodeToDuration(offset, max_scale);
                expression_range.start_time = subtractTimeseriesDuration(expression_range.start_time, offset_value);
                expression_range.end_time = subtractTimeseriesDuration(expression_range.end_time, offset_value);
            }
            visitNode(expression, expression_range, promql_tree, settings);
            break;
        }

        case PrometheusQueryTree::NodeType::Subquery:
        {
            const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
            auto max_scale = getTimeseriesScale(settings.result_timestamp_type);
            auto subquery_range = nodeToDuration(subquery_node->getRange(), max_scale);

            DecimalField<Decimal64> subquery_step;
            if (const auto * resolution_node = subquery_node->getResolution())
                subquery_step = nodeToDuration(resolution_node, max_scale);
            else
                subquery_step = settings.default_resolution;

            const auto * expression = subquery_node->getExpression();
            NodeEvaluationRange expression_range = range;

            expression_range.end_time = roundDownTimeseriesTime(range.end_time, subquery_step);

            auto unaligned_start_time = subtractTimeseriesDuration(range.start_time, subquery_range);
            expression_range.start_time = roundUpTimeseriesTime(unaligned_start_time, subquery_step);
            if (expression_range.start_time == unaligned_start_time)
                expression_range.start_time = addTimeseriesDuration(unaligned_start_time, subquery_step);

            expression_range.step = subquery_step;

            visitNode(expression, expression_range, promql_tree, settings);
            break;
        }

        default:
        {
            for (const auto * child : node->children)
                visitNode(child, range, promql_tree, settings);
        }
    }
}


void NodeEvaluationRangeGetter::setWindows(const PrometheusQueryEvaluationSettings & settings)
{
    auto max_scale = getTimeseriesScale(settings.result_timestamp_type);
    for (auto it = map.begin(); it != map.end(); ++it)
    {
        const auto * node = it->first;
        if (node->node_type == PrometheusQueryTree::NodeType::RangeSelector)
        {
            /// We've found a range selector, we propagate its window to a range-vector function
            /// (if this range selector is used in any range-vector function).
            const auto * range_selector_node = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
            auto window = nodeToDuration(range_selector_node->getRange(), max_scale);
            it->second.window = window;
            const auto * parent = node->parent;
            while (parent)
            {
                map.at(parent).window = window;
                if (parent->result_type != PrometheusQueryResultType::RANGE_VECTOR)
                    break;
                parent = parent->parent;
            }
        }
    }
}


const NodeEvaluationRange & NodeEvaluationRangeGetter::get(const PrometheusQueryTree::Node * node) const
{
    auto it = map.find(node);
    if (it == map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found node {} in NodeEvaluationRangeGetter", node->node_type);
    return it->second;
}

}
