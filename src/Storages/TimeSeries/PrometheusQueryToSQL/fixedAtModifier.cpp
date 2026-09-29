#include <Storages/TimeSeries/PrometheusQueryToSQL/fixedAtModifier.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <base/defines.h>


namespace DB::PrometheusQueryToSQL
{

const PrometheusQueryTree::Offset * getFixedAtModifier(const SQLQueryPiece & argument)
{
    if (argument.type != ResultType::RANGE_VECTOR || !argument.node || argument.node->node_type != NodeType::Offset)
        return nullptr;

    const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(argument.node);
    return offset_node->hasAtModifier() ? offset_node : nullptr;
}


NodeEvaluationRange getRangeAggregationRange(
    const SQLQueryPiece & range_argument, const NodeEvaluationRange & node_range, ConverterContext & context)
{
    const auto * fixed_at_node = getFixedAtModifier(range_argument);
    if (!fixed_at_node)
    {
        if (range_argument.type != ResultType::RANGE_VECTOR || !range_argument.node || range_argument.node->node_type != NodeType::Offset)
            return node_range;

        /// The offset expression is evaluated on the grid shifted back by the offset.
        const auto * offset_node = static_cast<const PrometheusQueryTree::Offset *>(range_argument.node);
        NodeEvaluationRange shifted_range = context.node_range_getter.get(offset_node->getExpression());
        shifted_range.window = node_range.window;
        return shifted_range;
    }

    /// Under a fixed @ modifier the range function's sample window is frozen at the fixed timestamp, while
    /// the range-vector argument retains its own inner grid.
    const auto & fixed_range = context.node_range_getter.get(fixed_at_node->getExpression());
    chassert(fixed_range.start_time == fixed_range.end_time);
    return NodeEvaluationRange{fixed_range.start_time, fixed_range.end_time, DurationType{0}, node_range.window};
}


ASTPtr repeatFixedAtResultOverGrid(
    ASTPtr && aggregate_values, const NodeEvaluationRange & aggregation_range, size_t result_grid_size)
{
    /// A fixed @ expression is evaluated once by Prometheus. Repeat the single aggregate result on the outer
    /// query grid instead of sliding the range function over the outer evaluation timestamps.
    const auto aggregation_grid_size
        = stepsInTimeSeriesRange(aggregation_range.start_time, aggregation_range.end_time, aggregation_range.step);

    return makeASTFunction(
        "arrayResize",
        make_intrusive<ASTLiteral>(Array{}),
        make_intrusive<ASTLiteral>(result_grid_size),
        makeASTFunction("arrayElement", std::move(aggregate_values), make_intrusive<ASTLiteral>(aggregation_grid_size)));
}

}
