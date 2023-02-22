#include <Analyzer/Passes/AggregateFunctionsArithmericOperationsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Functions/IFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

namespace
{

Field zeroField(const Field & value)
{
    switch (value.getType())
    {
        case Field::Types::UInt64: return static_cast<UInt64>(0);
        case Field::Types::Int64: return static_cast<Int64>(0);
        case Field::Types::Float64: return static_cast<Float64>(0);
        case Field::Types::UInt128: return static_cast<UInt128>(0);
        case Field::Types::Int128: return static_cast<Int128>(0);
        case Field::Types::UInt256: return static_cast<UInt256>(0);
        case Field::Types::Int256: return static_cast<Int256>(0);
        default:
            break;
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unexpected literal type in function");
}

/** Rewrites:   sum([multiply|divide]) -> [multiply|divide](sum)
  *             [min|max|avg]([multiply|divide|plus|minus]) -> [multiply|divide|plus|minus]([min|max|avg])
  *
  * TODO: Support `groupBitAnd`, `groupBitOr`, `groupBitXor` functions.
  * TODO: Support rewrite `f((2 * n) * n)` into '2 * f(n * n)'.
  */
class AggregateFunctionsArithmericOperationsVisitor : public InDepthQueryTreeVisitor<AggregateFunctionsArithmericOperationsVisitor>
{
public:
    /// Traverse tree bottom to top
    static bool shouldTraverseTopToBottom()
    {
        return false;
    }

    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * aggregate_function_node = node->as<FunctionNode>();
        if (!aggregate_function_node || !aggregate_function_node->isAggregateFunction())
            return;

        static std::unordered_map<std::string_view, std::unordered_set<std::string_view>> supported_functions
            = {{"sum", {"multiply", "divide"}},
               {"min", {"multiply", "divide", "plus", "minus"}},
               {"max", {"multiply", "divide", "plus", "minus"}},
               {"avg", {"multiply", "divide", "plus", "minus"}}};

        auto & aggregate_function_arguments_nodes = aggregate_function_node->getArguments().getNodes();
        if (aggregate_function_arguments_nodes.size() != 1)
            return;

        auto * inner_function_node = aggregate_function_arguments_nodes[0]->as<FunctionNode>();
        if (!inner_function_node)
            return;

        auto & inner_function_arguments_nodes = inner_function_node->getArguments().getNodes();
        if (inner_function_arguments_nodes.size() != 2)
            return;

        /// Aggregate functions[sum|min|max|avg] is case-insensitive, so we use lower cases name
        auto lower_function_name = Poco::toLower(aggregate_function_node->getFunctionName());

        auto supported_function_it = supported_functions.find(lower_function_name);
        if (supported_function_it == supported_functions.end())
            return;

        const auto & inner_function_name = inner_function_node->getFunctionName();

        if (!supported_function_it->second.contains(inner_function_name))
            return;

        auto left_argument_constant_value = inner_function_arguments_nodes[0]->getConstantValueOrNull();
        auto right_argument_constant_value = inner_function_arguments_nodes[1]->getConstantValueOrNull();

        /** If we extract negative constant, aggregate function name must be updated.
          *
          * Example: SELECT min(-1 * id);
          * Result: SELECT -1 * max(id);
          */
        std::string function_name_if_constant_is_negative;
        if (inner_function_name == "multiply" || inner_function_name == "divide")
        {
            if (lower_function_name == "min")
                function_name_if_constant_is_negative = "max";
            else if (lower_function_name == "max")
                function_name_if_constant_is_negative = "min";
        }

        if (left_argument_constant_value && !right_argument_constant_value)
        {
            /// Do not rewrite `sum(1/n)` with `sum(1) * div(1/n)` because of lose accuracy
            if (inner_function_name == "divide")
                return;

            /// Rewrite `aggregate_function(inner_function(constant, argument))` into `inner_function(constant, aggregate_function(argument))`
            const auto & left_argument_constant_value_literal = left_argument_constant_value->getValue();
            if (!function_name_if_constant_is_negative.empty() &&
                left_argument_constant_value_literal < zeroField(left_argument_constant_value_literal))
            {
                resolveAggregateFunctionNode(*aggregate_function_node, function_name_if_constant_is_negative);
            }

            auto inner_function = aggregate_function_arguments_nodes[0];
            auto inner_function_right_argument = std::move(inner_function_arguments_nodes[1]);
            aggregate_function_arguments_nodes = {inner_function_right_argument};
            inner_function_arguments_nodes[1] = node;
            node = std::move(inner_function);
        }
        else if (right_argument_constant_value)
        {
            /// Rewrite `aggregate_function(inner_function(argument, constant))` into `inner_function(aggregate_function(argument), constant)`
            const auto & right_argument_constant_value_literal = right_argument_constant_value->getValue();
            if (!function_name_if_constant_is_negative.empty() &&
                right_argument_constant_value_literal < zeroField(right_argument_constant_value_literal))
            {
                resolveAggregateFunctionNode(*aggregate_function_node, function_name_if_constant_is_negative);
            }

            auto inner_function = aggregate_function_arguments_nodes[0];
            auto inner_function_left_argument = std::move(inner_function_arguments_nodes[0]);
            aggregate_function_arguments_nodes = {inner_function_left_argument};
            inner_function_arguments_nodes[0] = node;
            node = std::move(inner_function);
        }
    }

private:
    static inline void resolveAggregateFunctionNode(FunctionNode & function_node, const String & aggregate_function_name)
    {
        auto function_result_type = function_node.getResultType();
        auto function_aggregate_function = function_node.getAggregateFunction();

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name,
            function_aggregate_function->getArgumentTypes(),
            function_aggregate_function->getParameters(),
            properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function), std::move(function_result_type));
    }
};

}

void AggregateFunctionsArithmericOperationsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    AggregateFunctionsArithmericOperationsVisitor visitor;
    visitor.visit(query_tree_node);
}

}
