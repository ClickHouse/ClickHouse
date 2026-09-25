#include <Analyzer/Passes/AggregateFunctionsArithmeticOperationsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/NaNUtils.h>

#include <Core/Settings.h>

#include <base/scope_guard.h>

namespace DB
{
namespace Setting
{
    extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
    extern const SettingsBool empty_result_for_aggregation_by_constant_keys_on_empty_set;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
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

bool hasNonConstantKey(const QueryTreeNodes & keys)
{
    for (const auto & key : keys)
    {
        if (!key->as<ConstantNode>())
            return true;
    }
    return false;
}

/// A keyless aggregation and the WITH TOTALS grand total emit a row even over empty input; a key set with a
/// non-constant key cannot. The empty-result settings suppress the keyless and all-constant-key rows, but never
/// the grand total, nor a keyless grouping set while another set has a key.
bool aggregationMayBeEmpty(const QueryNode & query_node, bool empty_result_for_empty_set, bool empty_result_for_constant_keys)
{
    if (query_node.isGroupByWithTotals())
        return true;

    if (!query_node.hasGroupBy())
        return !empty_result_for_empty_set;

    if (query_node.isGroupByWithGroupingSets())
    {
        for (const auto & grouping_set : query_node.getGroupBy().getNodes())
        {
            const auto * grouping_set_keys = grouping_set->as<ListNode>();
            if (!grouping_set_keys || !hasNonConstantKey(grouping_set_keys->getNodes()))
                return true;
        }

        return false;
    }

    /// ROLLUP and CUBE keep constant keys to derive their grouping levels, so their key set stays keyed.
    if (query_node.isGroupByWithRollup() || query_node.isGroupByWithCube())
        return false;

    if (hasNonConstantKey(query_node.getGroupBy().getNodes()))
        return false;

    return !(empty_result_for_empty_set || empty_result_for_constant_keys);
}

bool hasNonConstantGroupingKey(const QueryNode & query_node)
{
    if (!query_node.hasGroupBy())
        return false;

    if (!query_node.isGroupByWithGroupingSets())
        return hasNonConstantKey(query_node.getGroupBy().getNodes());

    for (const auto & grouping_set : query_node.getGroupBy().getNodes())
    {
        const auto * grouping_set_keys = grouping_set->as<ListNode>();
        if (grouping_set_keys && hasNonConstantKey(grouping_set_keys->getNodes()))
            return true;
    }
    return false;
}

bool constantKeepsZeroAtZero(const Field & value, bool zero_has_no_sign)
{
    switch (value.getType())
    {
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UInt256:
        case Field::Types::Int256:
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
            break;
        case Field::Types::Float64:
            if (!isFinite(value.safeGet<Float64>()))
                return false;
            break;
        default:
            return false;
    }

    const Field zero = zeroField(value);
    return zero < value || (zero_has_no_sign && value < zero);
}

enum class EmptyStateHoist
{
    Keep,
    Guard,
    Decline,
};

EmptyStateHoist hoistOverEmptyState(
    const String & arithmetic_function_name,
    const Field & constant_value,
    const IDataType & aggregate_result_type,
    bool aggregated_argument_is_nullable,
    bool aggregation_has_one_state,
    bool date_time_overflow_throws)
{
    /// An aggregate over a Nullable operand has NULL as its empty state, and NULL survives the operation.
    if (aggregated_argument_is_nullable)
        return EmptyStateHoist::Keep;

    /// Nullability can come from the other operand: then only the unrewritten aggregate is NULL over empty input.
    /// It can also come from a `Variant` operand, which `min` and `max` do not accept, so this is not guarded.
    if (aggregate_result_type.isNullable())
        return EmptyStateHoist::Decline;

    if (arithmetic_function_name == "plus" || arithmetic_function_name == "minus")
    {
        /// The guard adds a count() state to every group, and its hoisted expression is still computed from the empty
        /// state, where checked date arithmetic can overflow.
        if (!aggregation_has_one_state
            || (date_time_overflow_throws && isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(aggregate_result_type)))
            return EmptyStateHoist::Decline;
        return EmptyStateHoist::Guard;
    }

    /// In floating point a negative constant maps zero to a negative zero, which `1 / x` tells apart from zero.
    if (constantKeepsZeroAtZero(constant_value, WhichDataType(aggregate_result_type).isIntegerOrDecimal()))
        return EmptyStateHoist::Keep;

    return EmptyStateHoist::Decline;
}

Field emptyStateResult(const IAggregateFunction & function)
{
    Arena arena;
    AlignedBuffer place(function.sizeOfData(), function.alignOfData());
    function.create(place.data());
    SCOPE_EXIT(function.destroy(place.data()));

    auto column = function.getResultType()->createColumn();
    function.insertResultInto(place.data(), *column, &arena);
    return (*column)[0];
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
        if (const auto * query_node = node->as<QueryNode>())
            query_aggregations.push_back(
                {aggregationMayBeEmpty(
                     *query_node,
                     getSettings()[Setting::empty_result_for_aggregation_by_empty_set],
                     getSettings()[Setting::empty_result_for_aggregation_by_constant_keys_on_empty_set]),
                 !hasNonConstantGroupingKey(*query_node)});

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

        const auto query_aggregation = query_aggregations.empty() ? QueryAggregation{} : query_aggregations.back();
        const auto * hoisted_constant_node = right_argument_constant_node ? right_argument_constant_node : left_argument_constant_node;
        const auto & aggregated_argument_node
            = right_argument_constant_node ? arithmetic_function_arguments_nodes[0] : arithmetic_function_arguments_nodes[1];
        const auto empty_state_hoist = query_aggregation.may_be_empty
            ? hoistOverEmptyState(
                  arithmetic_function_name,
                  hoisted_constant_node->getValue(),
                  *aggregate_function_node->getResultType(),
                  aggregated_argument_node->getResultType()->isNullable(),
                  query_aggregation.has_one_state,
                  getSettings()[Setting::date_time_overflow_behavior] == FormatSettings::DateTimeOverflowBehavior::Throw)
            : EmptyStateHoist::Keep;
        if (empty_state_hoist == EmptyStateHoist::Decline)
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
        if (!optimized_function_node->getResultType()->equals(*node->getResultType()))
            return;

        /// Within a guard's hoisted expression the aggregation is non-empty wherever its value is used.
        const bool inside_guard = !open_guards.empty() && open_guards.back().second == query_aggregations.size();
        if (empty_state_hoist == EmptyStateHoist::Guard && !inside_guard)
        {
            optimized_function_node = guardEmptyState(std::move(optimized_function_node), *aggregate_function_node);
            open_guards.emplace_back(&node, query_aggregations.size());
        }

        node = std::move(optimized_function_node);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!open_guards.empty() && open_guards.back().first == &node)
            open_guards.pop_back();

        if (node->as<QueryNode>())
            query_aggregations.pop_back();
    }

private:
    struct QueryAggregation
    {
        bool may_be_empty = true;
        bool has_one_state = false;
    };

    std::vector<QueryAggregation> query_aggregations;
    std::vector<std::pair<const QueryTreeNodePtr *, size_t>> open_guards;

    /// if(count() = 0, <result of the unrewritten aggregate over an empty state>, hoisted)
    QueryTreeNodePtr guardEmptyState(QueryTreeNodePtr hoisted_node, const FunctionNode & aggregate_function_node)
    {
        auto count_node = std::make_shared<FunctionNode>("count");
        resolveAggregateFunctionNodeByName(*count_node, count_node->getFunctionName());

        auto is_empty_node = std::make_shared<FunctionNode>("equals");
        is_empty_node->markAsOperator();
        is_empty_node->getArguments().getNodes() = {std::move(count_node), std::make_shared<ConstantNode>(static_cast<UInt64>(0))};
        resolveOrdinaryFunctionNodeByName(*is_empty_node, is_empty_node->getFunctionName(), getContext());

        auto empty_state_result_node = std::make_shared<ConstantNode>(
            emptyStateResult(*aggregate_function_node.getAggregateFunction()), aggregate_function_node.getResultType());

        auto guard_node = std::make_shared<FunctionNode>("if");
        guard_node->getArguments().getNodes() = {std::move(is_empty_node), std::move(empty_state_result_node), std::move(hoisted_node)};
        resolveOrdinaryFunctionNodeByName(*guard_node, guard_node->getFunctionName(), getContext());
        return guard_node;
    }

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
