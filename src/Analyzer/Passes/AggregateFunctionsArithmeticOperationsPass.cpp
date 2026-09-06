#include <Analyzer/Passes/AggregateFunctionsArithmeticOperationsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <Common/FieldAccurateComparison.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_arithmetic_operations_in_aggregate_functions;
}


namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

namespace
{

/** A `Decimal` operand makes the arithmetic compute in the decimal's own native signed width
  * (`Int32` for every `Decimal32`, `Int64` for every `Decimal64`, and so on), into which the other
  * operand is materialised by a `static_cast`. A constant outside that width participates as a
  * different value: `Decimal32 * 9223372036854775807` multiplies by `-1`.
  *
  * The rewrite must not fire then: it decides the `min`/`max` swap from the literal (which has the
  * opposite sign of the value the query actually multiplies by) and it moves the operation into the
  * wider result type of the aggregate, where the constant survives and the result differs.
  */
bool constantTruncatesIntoDecimalWidth(const DataTypePtr & argument_type, const DataTypePtr & constant_type, const Field & constant)
{
    const auto argument_type_without_wrappers = removeNullable(removeLowCardinality(argument_type));
    if (!isDecimal(argument_type_without_wrappers) || !isInteger(removeNullable(removeLowCardinality(constant_type))))
        return false;

    auto exceeds = [&constant]<typename T>(std::type_identity<T>)
    {
        return accurateLess(constant, Field(std::numeric_limits<T>::min()))
            || accurateLess(Field(std::numeric_limits<T>::max()), constant);
    };

    switch (argument_type_without_wrappers->getSizeOfValueInMemory())
    {
        case sizeof(Int32): return exceeds(std::type_identity<Int32>{});
        case sizeof(Int64): return exceeds(std::type_identity<Int64>{});
        case sizeof(Int128): return exceeds(std::type_identity<Int128>{});
        case sizeof(Int256): return exceeds(std::type_identity<Int256>{});
        default: return true; /// unreachable for the four `Decimal` widths above; fails close if a new one appears
    }
}

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
        case Field::Types::Decimal32: return static_cast<Decimal32>(0);
        case Field::Types::Decimal64: return static_cast<Decimal64>(0);
        case Field::Types::Decimal128: return static_cast<Decimal128>(0);
        case Field::Types::Decimal256: return static_cast<Decimal256>(0);
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
class AggregateFunctionsArithmeticOperationsVisitor : public InDepthQueryTreeVisitorWithContext<AggregateFunctionsArithmeticOperationsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<AggregateFunctionsArithmeticOperationsVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_arithmetic_operations_in_aggregate_functions])
            return;

        auto * aggregate_function_node = node->as<FunctionNode>();
        if (!aggregate_function_node || !aggregate_function_node->isAggregateFunction())
            return;

        static std::unordered_map<std::string_view, std::unordered_set<std::string_view>> supported_aggregate_functions
            = {{"sum", {"multiply", "divide"}},
               {"min", {"multiply", "divide", "plus", "minus"}},
               {"max", {"multiply", "divide", "plus", "minus"}},
               {"avg", {"multiply", "divide", "plus", "minus"}}};

        auto & aggregate_function_arguments_nodes = aggregate_function_node->getArguments().getNodes();
        if (aggregate_function_arguments_nodes.size() != 1)
            return;

        const auto & arithmetic_function_node = aggregate_function_arguments_nodes[0];
        auto * arithmetic_function_node_typed = arithmetic_function_node->as<FunctionNode>();
        if (!arithmetic_function_node_typed)
            return;

        const auto & arithmetic_function_arguments_nodes = arithmetic_function_node_typed->getArguments().getNodes();
        if (arithmetic_function_arguments_nodes.size() != 2)
            return;

        /// Aggregate functions[sum|min|max|avg] is case-insensitive, so we use lower cases name
        auto lower_aggregate_function_name = Poco::toLower(aggregate_function_node->getFunctionName());

        auto supported_aggregate_function_it = supported_aggregate_functions.find(lower_aggregate_function_name);
        if (supported_aggregate_function_it == supported_aggregate_functions.end())
            return;

        const auto & arithmetic_function_name = arithmetic_function_node_typed->getFunctionName();
        if (!supported_aggregate_function_it->second.contains(arithmetic_function_name))
            return;

        const auto * left_argument_constant_node = arithmetic_function_arguments_nodes[0]->as<ConstantNode>();
        const auto * right_argument_constant_node = arithmetic_function_arguments_nodes[1]->as<ConstantNode>();

        if (!left_argument_constant_node && !right_argument_constant_node)
            return;

        /** Need reverse max <-> min for:
          *
          * max(-1*value) -> -1*min(value)
          * max(value/-2) -> min(value)/-2
          * max(1-value) -> 1-min(value)
          */
        auto get_reverse_aggregate_function_name = [](const std::string & aggregate_function_name) -> std::string
        {
            if (aggregate_function_name == "min")
                return "max";
            if (aggregate_function_name == "max")
                return "min";
            return aggregate_function_name;
        };

        size_t arithmetic_function_argument_index = 0;

        if (left_argument_constant_node && !right_argument_constant_node)
        {
            /// Do not rewrite `sum(1/n)` with `sum(1) * div(1/n)` because of lose accuracy
            if (arithmetic_function_name == "divide")
                return;

            /// Rewrite `aggregate_function(inner_function(constant, argument))` into `inner_function(constant, aggregate_function(argument))`
            const auto & left_argument_constant_value_literal = left_argument_constant_node->getValue();

            if (constantTruncatesIntoDecimalWidth(
                    arithmetic_function_arguments_nodes[1]->getResultType(),
                    left_argument_constant_node->getResultType(),
                    left_argument_constant_value_literal))
                return;

            bool need_reverse = (arithmetic_function_name == "multiply" && left_argument_constant_value_literal < zeroField(left_argument_constant_value_literal))
                || (arithmetic_function_name == "minus");

            if (need_reverse)
                lower_aggregate_function_name = get_reverse_aggregate_function_name(lower_aggregate_function_name);

            arithmetic_function_argument_index = 1;
        }
        else if (right_argument_constant_node)
        {
            /// Rewrite `aggregate_function(inner_function(argument, constant))` into `inner_function(aggregate_function(argument), constant)`
            const auto & right_argument_constant_value_literal = right_argument_constant_node->getValue();

            if (constantTruncatesIntoDecimalWidth(
                    arithmetic_function_arguments_nodes[0]->getResultType(),
                    right_argument_constant_node->getResultType(),
                    right_argument_constant_value_literal))
                return;

            bool need_reverse = (arithmetic_function_name == "multiply" || arithmetic_function_name == "divide") && right_argument_constant_value_literal < zeroField(right_argument_constant_value_literal);

            if (need_reverse)
                lower_aggregate_function_name = get_reverse_aggregate_function_name(lower_aggregate_function_name);

            arithmetic_function_argument_index = 0;
        }

        auto optimized_function_node = cloneArithmeticFunctionAndWrapArgumentIntoAggregateFunction(arithmetic_function_node,
            arithmetic_function_argument_index,
            node,
            lower_aggregate_function_name);
        if (optimized_function_node->getResultType()->equals(*node->getResultType()))
            node = std::move(optimized_function_node);
    }

private:
    QueryTreeNodePtr cloneArithmeticFunctionAndWrapArgumentIntoAggregateFunction(
        const QueryTreeNodePtr & arithmetic_function,
        size_t arithmetic_function_argument_index,
        const QueryTreeNodePtr & aggregate_function,
        const std::string & result_aggregate_function_name)
    {
        auto arithmetic_function_clone = arithmetic_function->clone();
        auto & arithmetic_function_clone_typed = arithmetic_function_clone->as<FunctionNode &>();
        auto & arithmetic_function_clone_arguments_nodes = arithmetic_function_clone_typed.getArguments().getNodes();
        auto & arithmetic_function_clone_argument = arithmetic_function_clone_arguments_nodes[arithmetic_function_argument_index];

        auto aggregate_function_clone = aggregate_function->clone();
        auto & aggregate_function_clone_typed = aggregate_function_clone->as<FunctionNode &>();

        aggregate_function_clone_typed.getArguments().getNodes() = { arithmetic_function_clone_argument };
        resolveAggregateFunctionNodeByName(aggregate_function_clone_typed, result_aggregate_function_name);

        arithmetic_function_clone_arguments_nodes[arithmetic_function_argument_index] = std::move(aggregate_function_clone);
        resolveOrdinaryFunctionNodeByName(arithmetic_function_clone_typed, arithmetic_function_clone_typed.getFunctionName(), getContext());

        return arithmetic_function_clone;
    }
};

}

void AggregateFunctionsArithmeticOperationsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    AggregateFunctionsArithmeticOperationsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
