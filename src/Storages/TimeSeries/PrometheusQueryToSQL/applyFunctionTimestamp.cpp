#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionTimestamp.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
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
    /// Peels off AST shapes that don't change the identity of the underlying instant selector's samples - a
    /// unary +/-, a binary operation against a scalar literal, or a nested timestamp() call - to find the bare
    /// instant selector underneath, if any. Returns nullptr if `node` doesn't reduce to a bare instant selector
    /// this way (e.g. it's an aggregation, a binary operation of two vectors, another function call, etc.).
    const PQT::InstantSelector * peelToInstantSelector(const Node * node)
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
                return peelToInstantSelector(unary_operator->getArgument());
            }

            case NodeType::BinaryOperator:
            {
                const auto * binary_operator = static_cast<const PQT::BinaryOperator *>(node);
                const Node * left = binary_operator->getLeftArgument();
                const Node * right = binary_operator->getRightArgument();
                bool left_is_scalar = left->node_type == NodeType::Scalar;
                bool right_is_scalar = right->node_type == NodeType::Scalar;
                if (left_is_scalar == right_is_scalar)
                    return nullptr; /// Need exactly one scalar literal operand.
                return peelToInstantSelector(left_is_scalar ? right : left);
            }

            case NodeType::Function:
            {
                const auto * function = static_cast<const PQT::Function *>(node);
                if (function->function_name != "timestamp" || function->getArguments().size() != 1)
                    return nullptr;
                return peelToInstantSelector(function->getArguments()[0]);
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
    const auto * instant_selector = peelToInstantSelector(function_node->getArguments().at(0));
    if (!instant_selector)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function_node->function_name);

    auto instant_selector_text = instant_selector->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector, context);
    auto res = applyFunctionOverRange(instant_selector, "timestamp", {std::move(range_selector)}, context);
    res.node = function_node;
    return res;
}

}
