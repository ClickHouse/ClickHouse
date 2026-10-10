#include <Analyzer/Passes/AggregateFunctionsArithmeticOperationsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Common/NaNUtils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeInterval.h>
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

/// `x * value` and `x / value` keep the order of `x` and distribute over `sum` only for a finite non-zero `value`.
bool isFiniteNonZero(const Field & value)
{
    if (value.isNull())
        return false;
    if (value.getType() == Field::Types::Float64)
        return isFinite(value.safeGet<Float64>()) && value.safeGet<Float64>() != 0;
    return value != zeroField(value);
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

        /** Arithmetic with a `Decimal` operand does not distribute over these aggregate functions. It computes
          * in the native width of the decimal, into which the other operand is truncated, with an overflow
          * check on every row, and `divide` drops the fractional digits beyond the scale of the decimal on
          * every row. The hoisted operation runs once, on the aggregate, often in a wider type: for
          * `a Decimal32(0)`, `sum(a / 2)` over `{1, 1}` is `0`, but `sum(a) / 2` is `1`, and `sum(a * 3)`
          * throws `DECIMAL_OVERFLOW` for a row `999999999`, but `sum(a) * 3` does not.
          */
        /// Nor with an operand that is not an integer or a float, except a date, a time or an interval under `min` or `max`:
        /// `min` and `max` order an `Array` or a `Tuple` lexicographically, which an element-wise operation does not preserve,
        /// `sum` and `avg` reject an IP address, and `avg` of a date, a time or an interval is rounded to its unit.
        const bool is_min_or_max = lower_aggregate_function_name == "min" || lower_aggregate_function_name == "max";
        bool has_date_time_argument = false;
        bool has_day_or_longer_interval = false;
        for (const auto & argument : arithmetic_function_arguments_nodes)
        {
            const auto argument_type = removeNullable(removeLowCardinality(argument->getResultType()));
            const WhichDataType which(argument_type);
            if (!which.isInteger() && !which.isFloat()
                && !(is_min_or_max && (which.isInterval() || which.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64())))
                return;

            has_date_time_argument |= which.isDateTimeOrDateTime64() || which.isTime64();
            if (const auto * interval_type = typeid_cast<const DataTypeInterval *>(argument_type.get()))
                has_day_or_longer_interval |= interval_type->getKind() >= IntervalKind(IntervalKind::Kind::Day);
        }

        /// A day or longer is added to a `DateTime` or a `Time64` in the calendar of its time zone, which is not monotone
        /// across a DST change or a month end: a month before both `12-30 23:55` and `12-31 00:00` is on `11-30`.
        if (has_date_time_argument && has_day_or_longer_interval)
            return;

        const auto * constant_node = right_argument_constant_node ? right_argument_constant_node : left_argument_constant_node;
        if ((arithmetic_function_name == "multiply" || arithmetic_function_name == "divide") && !isFiniteNonZero(constant_node->getValue()))
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
