#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionTimestamp.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyComparisonOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Returns the InstantSelector if `node` is a bare InstantSelector or an Offset node
    /// directly wrapping a bare InstantSelector; returns nullptr for any other expression
    /// (unary/binary operators, aggregations, functions, etc.), matching Prometheus 3.5.0
    /// which only supports timestamp() on direct vector selectors.
    const PQT::InstantSelector * peelToInstantSelector(const Node * node, const PQT::Offset *& offset_node)
    {
        if (node->node_type == NodeType::InstantSelector)
        {
            return static_cast<const PQT::InstantSelector *>(node);
        }
        if (node->node_type == NodeType::Offset)
        {
            const auto * offset = static_cast<const PQT::Offset *>(node);
            if (offset->getExpression()->node_type == NodeType::InstantSelector)
            {
                offset_node = offset;
                return static_cast<const PQT::InstantSelector *>(offset->getExpression());
            }
        }
        return nullptr;
    }
}


bool isFunctionTimestamp(std::string_view function_name)
{
    return function_name == "timestamp";
}


SQLQueryPiece applyFunctionTimestamp(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    if (arguments.size() != 1)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function 'timestamp' expects 1 argument, but was called with {} arguments",
                        arguments.size());
    }

    /// `arguments[0]` is the already-converted SQL representation of the argument's *value*, built eagerly by
    /// Converter::visitNode() before applyFunction() gets called - that's not what we need here, because once an
    /// instant vector goes through an arbitrary operator or function its individual samples' original timestamps
    /// aren't tracked through the conversion. So we independently inspect the raw argument AST to check if it is
    /// a bare instant selector (or direct offset/@ wrapper) whose samples' timestamps we can read directly.
    const PQT::Offset * offset_node = nullptr;
    const auto * instant_selector = peelToInstantSelector(function_node->getArguments().at(0), offset_node);
    if (!instant_selector)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function_node->function_name);

    /// `instant_selector`'s evaluation range (used both below and inside fromRangeSelector()) already accounts for
    /// the offset/@ modifier if any - NodeEvaluationRangeGetter shifts it for every node nested under an Offset
    /// node. So the raw samples we read here, and the window we compute timestamp() over, are already correctly
    /// shifted, and the timestamps this returns are the samples' genuine, unshifted timestamps, exactly as
    /// PromQL's timestamp() requires. What's left is to re-apply the offset/@ modifier the same way applyOffset()
    /// does for every other expression, to move the result's grid alignment back from the (shifted) selector's
    /// evaluation range to the outer query's grid - without touching the already-correct timestamp values.
    auto instant_selector_text = instant_selector->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector, context);
    auto res = applyFunctionOverRange(instant_selector, "timestamp", {std::move(range_selector)}, context);
    if (offset_node)
        res = applyOffset(offset_node, std::move(res), context);
    res.node = function_node;
    return res;
}

}
