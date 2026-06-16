#include <Analyzer/Passes/RewriteAggregateFunctionWithIfPass.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Core/Settings.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_aggregate_function_with_if;
}

namespace
{

/// An aggregate preserves NULL payload rows when its null handling keeps them instead of skipping
/// them: rewriting f(if(cond, x, NULL)) -> fIf(x, cond) drops those rows and is only valid for
/// NULL-skipping aggregates. The *_respect_nulls family keeps them and signals this by returning
/// itself from getOwnNullAdapter (the Null combinator's "do not wrap me, I keep NULL rows"
/// contract). NULL-skipping aggregates return a wrapping adapter or nullptr.
bool aggregateFunctionPreservesNullPayload(const AggregateFunctionPtr & function)
{
    if (!function)
        return false;

    /// Look through combinators (-OrNull / -OrDefault / -Distinct / -State / ...): they inherit
    /// NULL-row handling from the function they wrap, so only the wrapped leaf can belong to the
    /// *_respect_nulls family. Without this, first_value_respect_nullsOrNull(...) would be rewritten.
    if (auto nested = function->getNestedFunction())
        return aggregateFunctionPreservesNullPayload(nested);

    /// The *_respect_nulls family is unary; a nullary aggregate (count()) cannot belong to it.
    const auto & argument_types = function->getArgumentTypes();
    if (argument_types.size() != 1)
        return false;

    /// canContainNull() (not isNullable()): Variant/Dynamic carry NULL via a discriminator without
    /// being wrapped in Nullable, so makeNullableSafe() leaves them unchanged. The rewrite is safe
    /// only when the argument cannot hold a NULL payload.
    if (!canContainNull(*argument_types[0]))
        return false;

    /// Probe with a Nullable argument, exactly as the Null combinator consults getOwnNullAdapter.
    /// count's adapter (AggregateFunctionCountNotNullUnary) rejects a non-Nullable argument, and
    /// Variant/Dynamic cannot be wrapped in Nullable, so fall back to a plain Nullable stand-in. The
    /// respect_nulls adapters ignore the argument and count only checks isNullable(), so the
    /// substitution does not change which adapter is returned.
    auto nullable_argument_type = makeNullableSafe(argument_types[0]);
    if (!nullable_argument_type->isNullable())
        nullable_argument_type = makeNullable(std::make_shared<DataTypeUInt8>());

    AggregateFunctionProperties properties;
    return function->getOwnNullAdapter(function, {nullable_argument_type}, function->getParameters(), properties) == function;
}

class RewriteAggregateFunctionWithIfVisitor : public InDepthQueryTreeVisitorWithContext<RewriteAggregateFunctionWithIfVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteAggregateFunctionWithIfVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_aggregate_function_with_if])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction())
            return;

        auto lower_name = Poco::toLower(function_node->getFunctionName());
        if (lower_name.ends_with("if"))
            return;

        const auto & function_arguments_nodes = function_node->getArguments().getNodes();
        if (function_arguments_nodes.size() != 1)
            return;

        auto * if_node = function_arguments_nodes[0]->as<FunctionNode>();
        if (!if_node || if_node->getFunctionName() != "if")
            return;

        /// The rewrite f(if(cond, x, NULL)) -> fIf(x, cond) turns the NULL branch into rows the -If
        /// condition skips, which is only valid when that NULL is a Nullable NULL the aggregate's null
        /// handling discards. Variant and Dynamic absorb the NULL as a discriminator value (a real
        /// payload row), so even NULL-skipping aggregates like count and any still process it; dropping
        /// it via the condition changes the result (count(if(number = 0, NULL, number::Variant(...)))
        /// goes from 4 to 3). Skip the rewrite whenever the if result can contain NULL but is not
        /// Nullable, regardless of the aggregate.
        auto if_result_type = if_node->getResultType();
        if (canContainNull(*if_result_type) && !isNullableOrLowCardinalityNullable(if_result_type))
            return;

        /// Do not rewrite aggregates that preserve NULL payload rows (the *_respect_nulls family,
        /// directly or wrapped in a combinator): the -If form drops those rows and changes the result.
        if (aggregateFunctionPreservesNullPayload(function_node->getAggregateFunction()))
            return;

        FunctionNodePtr replaced_node;

        auto if_arguments_nodes = if_node->getArguments().getNodes();
        auto * first_const_node = if_arguments_nodes[1]->as<ConstantNode>();
        auto * second_const_node = if_arguments_nodes[2]->as<ConstantNode>();
        if (second_const_node)
        {
            const auto & second_const_value = second_const_node->getValue();
            if (second_const_value.isNull()
                || (lower_name == "sum" && isInt64OrUInt64FieldType(second_const_value.getType()) && second_const_value.safeGet<UInt64>() == 0
                    && !if_node->getResultType()->isNullable()))
            {
                /// avg(if(cond, a, null)) -> avgIf(a::ResultTypeIf, cond)
                /// avg(if(cond, nullable_a, null)) -> avgIf(nullable_a, cond)
                /// sum(if(cond, a, 0)) -> sumIf(a, cond)
                /// sum(if(cond, nullable_a, 0)) **is not** equivalent to sumIfOrNull(cond, nullable_a) as
                ///     it changes the output when no rows pass the condition (from 0 to NULL)

                QueryTreeNodes new_arguments{2};

                /// We need to preserve the output type from if(). Notice that the return type of count() is the same either way
                if (if_arguments_nodes[1]->getResultType()->getName() != if_node->getResultType()->getName() && lower_name != "count")
                {
                    /// For constant arguments, directly convert to Nullable instead of
                    /// wrapping in _CAST. This avoids column name mismatches in distributed
                    /// queries where the _CAST FunctionNode (on initiator) vs folded
                    /// ConstantNode (on remote) produce different type annotations.
                    if (if_arguments_nodes[1]->as<ConstantNode>())
                    {
                        auto converted = if_arguments_nodes[1]->clone();
                        converted->as<ConstantNode &>().convertToNullable();
                        new_arguments[0] = std::move(converted);
                    }
                    else
                    {
                        new_arguments[0] = createCastFunction(std::move(if_arguments_nodes[1]), if_node->getResultType(), getContext());
                    }
                }
                else
                    new_arguments[0] = std::move(if_arguments_nodes[1]);

                new_arguments[1] = std::move(if_arguments_nodes[0]);

                replaced_node = std::make_shared<FunctionNode>(function_node->getFunctionName() + "If");
                replaced_node->getArguments().getNodes() = std::move(new_arguments);
                replaced_node->getParameters().getNodes() = function_node->getParameters().getNodes();
                resolveAggregateFunctionNodeByName(*replaced_node, replaced_node->getFunctionName());
            }
        }
        else if (first_const_node)
        {
            const auto & first_const_value = first_const_node->getValue();
            if (first_const_value.isNull()
                || (lower_name == "sum" && isInt64OrUInt64FieldType(first_const_value.getType()) && first_const_value.safeGet<UInt64>() == 0
                    && !if_node->getResultType()->isNullable()))
            {
                /// avg(if(cond, null, a) -> avgIf(a::ResultTypeIf, !cond))
                /// sum(if(cond, 0, a) -> sumIf(a, !cond))
                /// sum(if(cond, 0, nullable_a) **is not** sumIf(a, !cond)) -> Same as above

                QueryTreeNodes new_arguments{2};

                /// We need to preserve the output type from if(). Notice that the return type of count() is the same either way
                if (if_arguments_nodes[2]->getResultType()->getName() != if_node->getResultType()->getName() && lower_name != "count")
                {
                    if (if_arguments_nodes[2]->as<ConstantNode>())
                    {
                        auto converted = if_arguments_nodes[2]->clone();
                        converted->as<ConstantNode &>().convertToNullable();
                        new_arguments[0] = std::move(converted);
                    }
                    else
                    {
                        new_arguments[0] = createCastFunction(std::move(if_arguments_nodes[2]), if_node->getResultType(), getContext());
                    }
                }
                else
                    new_arguments[0] = std::move(if_arguments_nodes[2]);

                auto not_function = std::make_shared<FunctionNode>("not");
                not_function->markAsOperator();
                auto & not_function_arguments = not_function->getArguments().getNodes();
                not_function_arguments.push_back(std::move(if_arguments_nodes[0]));
                not_function->resolveAsFunction(
                    FunctionFactory::instance().get("not", getContext())->build(not_function->getArgumentColumns()));
                new_arguments[1] = std::move(not_function);

                replaced_node = std::make_shared<FunctionNode>(function_node->getFunctionName() + "If");
                replaced_node->getArguments().getNodes() = std::move(new_arguments);
                replaced_node->getParameters().getNodes() = function_node->getParameters().getNodes();
                resolveAggregateFunctionNodeByName(*replaced_node, replaced_node->getFunctionName());
            }
        }

        if (!replaced_node)
            return;

        auto prev_type = function_node->getResultType();
        auto curr_type = replaced_node->getResultType();
        if (!prev_type->equals(*curr_type))
            return;

        /// Just in case, CAST compatible aggregate function states.
        if (WhichDataType(prev_type).isAggregateFunction() && !DataTypeAggregateFunction::strictEquals(prev_type, curr_type))
            node = createCastFunction(std::move(replaced_node), prev_type, getContext());
        else
            node = std::move(replaced_node);
    }
};

}


void RewriteAggregateFunctionWithIfPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteAggregateFunctionWithIfVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
