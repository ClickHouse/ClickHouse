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
    /// Returns whether `node` is a scalar literal - a bare Scalar, or a unary +/- applied to one, since PromQL's
    /// parser represents signed literals (e.g. `-1`, `+2`) as a UnaryOperator wrapping a Scalar rather than as a
    /// Scalar node directly.
    bool isScalarLiteral(const Node * node)
    {
        if (node->node_type == NodeType::Scalar)
            return true;
        if (node->node_type == NodeType::UnaryOperator)
        {
            const auto * unary_operator = static_cast<const PQT::UnaryOperator *>(node);
            if ((unary_operator->operator_name == "+" || unary_operator->operator_name == "-")
                && unary_operator->getArgument()->node_type == NodeType::Scalar)
                return true;
        }
        return false;
    }

    /// Peels off AST shapes that don't change the identity of the underlying instant selector's samples - a
    /// unary +/-, a binary operation against a scalar literal, an offset/@ modifier, or a nested timestamp() call
    /// - to find the bare instant selector underneath, if any. Returns nullptr if `node` doesn't reduce to a bare
    /// instant selector this way (e.g. it's an aggregation, a binary operation of two vectors, another function
    /// call, etc.).
    ///
    /// If the peeled chain goes through offset/@ modifiers, `offset_nodes` collects those Offset nodes so the
    /// caller can re-apply them in order (to restore their effect on the evaluation range and the result's grid
    /// alignment) after computing the timestamp() result over the bare selector.
    const PQT::InstantSelector * peelToInstantSelector(const Node * node, std::vector<const PQT::Offset *> & offset_nodes)
    {
        switch (node->node_type)
        {
            case NodeType::InstantSelector:
                return static_cast<const PQT::InstantSelector *>(node);

            case NodeType::UnaryOperator:
            {
                const auto * unary_operator = static_cast<const PQT::UnaryOperator *>(node);
                if (unary_operator->operator_name != "+" && unary_operator->operator_name != "-")
                    return nullptr;
                return peelToInstantSelector(unary_operator->getArgument(), offset_nodes);
            }

            case NodeType::BinaryOperator:
            {
                const auto * binary_operator = static_cast<const PQT::BinaryOperator *>(node);
                const Node * left = binary_operator->getLeftArgument();
                const Node * right = binary_operator->getRightArgument();
                bool left_is_scalar_literal = isScalarLiteral(left);
                bool right_is_scalar_literal = isScalarLiteral(right);
                if (left_is_scalar_literal == right_is_scalar_literal)
                    return nullptr; /// Need exactly one scalar literal operand.

                /// Only operators that can't change which samples are present in the result may be peeled through:
                /// - math operators (+, -, *, /, %, ^, atan2) against a scalar always preserve every sample.
                /// - comparison operators (>, <, ==, ...) against a scalar only preserve every sample when the
                ///   `bool` modifier is used - that makes them behave like math operators (replacing the value with
                ///   0/1 without filtering). Without `bool` they filter out samples that don't match, so peeling
                ///   through them would incorrectly keep the selector's raw timestamp for a filtered-out sample.
                std::string_view operator_name = binary_operator->operator_name;
                bool preserves_every_sample = isMathBinaryOperator(operator_name)
                    || (isComparisonOperator(operator_name) && binary_operator->bool_modifier);
                if (!preserves_every_sample)
                    return nullptr;

                return peelToInstantSelector(left_is_scalar_literal ? right : left, offset_nodes);
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PQT::Function *>(node);
                if (function->function_name != "timestamp" || function->getArguments().size() != 1)
                    return nullptr;
                return peelToInstantSelector(function->getArguments()[0], offset_nodes);
            }

            case NodeType::Offset:
            {
                const auto * offset = static_cast<const PQT::Offset *>(node);
                offset_nodes.push_back(offset);
                return peelToInstantSelector(offset->getExpression(), offset_nodes);
            }

            default:
                return nullptr;
        }
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
    /// aren't tracked through the conversion. So we independently re-walk the raw argument AST instead, to see
    /// whether it reduces to a bare instant selector whose samples' timestamps we can read directly.
    std::vector<const PQT::Offset *> offset_nodes;
    const auto * instant_selector = peelToInstantSelector(function_node->getArguments().at(0), offset_nodes);
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
    for (auto it = offset_nodes.rbegin(); it != offset_nodes.rend(); ++it)
        res = applyOffset(*it, std::move(res), context);
    res.node = function_node;
    return res;
}

}
