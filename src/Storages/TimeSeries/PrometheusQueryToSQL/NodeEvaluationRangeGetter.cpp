#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTime.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

NodeEvaluationRangeGetter::NodeEvaluationRangeGetter(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                                     const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_scale(getResultTimeScale(settings_))
{
    const auto * root = promql_tree->getRoot();
    if (root)
    {
        if (settings_.evaluation_time)
        {
            NodeEvaluationRange range{
                .start_time = *settings_.evaluation_time,
                .end_time = *settings_.evaluation_time,
                .step = DecimalField<Decimal64>{},
                .window = settings_.lookback_delta};
            visitNode(root, range, settings_);
        }
        else if (settings_.evaluation_range)
        {
            NodeEvaluationRange range{
                .start_time = settings_.evaluation_range->start_time,
                .end_time = settings_.evaluation_range->end_time,
                .step = settings_.evaluation_range->step,
                .window = settings_.lookback_delta};
            visitNode(root, range, settings_);
        }
    }
    setWindows();
}


void NodeEvaluationRangeGetter::visitNode(
    const Node * node,
    const NodeEvaluationRange & range,
    const PrometheusQueryEvaluationSettings & settings)
{
    map[node] = range;
    visitChildren(node, range, settings);
}


void NodeEvaluationRangeGetter::visitChildren(
    const Node * node,
    const NodeEvaluationRange & range,
    const PrometheusQueryEvaluationSettings & settings)
{
    switch (node->node_type)
    {
        case NodeType::At:
        {
            const auto * at_node = static_cast<const PQT::At *>(node);
            const auto * expression = at_node->getExpression();
            NodeEvaluationRange expression_range = range;
            if (const auto * at = at_node->getAt())
            {
                auto timestamp = nodeToTime(at, time_scale);
                if (const auto * offset = at_node->getOffset())
                {
                    auto offset_value = nodeToDuration(offset, time_scale);
                    timestamp = subtractTimeseriesDuration(timestamp, offset_value);
                }
                expression_range.start_time = timestamp;
                expression_range.end_time = timestamp;
            }
            else if (const auto * offset = at_node->getOffset())
            {
                auto offset_value = nodeToDuration(offset, time_scale);
                expression_range.start_time = subtractTimeseriesDuration(expression_range.start_time, offset_value);
                expression_range.end_time = subtractTimeseriesDuration(expression_range.end_time, offset_value);
            }
            visitNode(expression, expression_range, settings);
            break;
        }

        case NodeType::Subquery:
        {
            const auto * subquery_node = static_cast<const PQT::Subquery *>(node);
            auto subquery_range = nodeToDuration(subquery_node->getRange(), time_scale);

            DecimalField<Decimal64> subquery_step;
            if (const auto * resolution_node = subquery_node->getResolution())
                subquery_step = nodeToDuration(resolution_node, time_scale);
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

            visitNode(expression, expression_range, settings);
            break;
        }

        default:
        {
            for (const auto * child : node->children)
                visitNode(child, range, settings);
        }
    }
}


void NodeEvaluationRangeGetter::setWindows()
{
    for (auto it = map.begin(); it != map.end(); ++it)
    {
        const auto * node = it->first;
        if (node->node_type == NodeType::RangeSelector)
        {
            /// We've found a range selector, we propagate its window to a range-vector function
            /// (if this range selector is used in any range-vector function).
            const auto * range_selector_node = static_cast<const PQT::RangeSelector *>(node);
            auto window = nodeToDuration(range_selector_node->getRange(), time_scale);
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
