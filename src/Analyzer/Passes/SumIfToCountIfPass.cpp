#include <Analyzer/Passes/SumIfToCountIfPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool aggregate_functions_null_for_empty;
    extern const SettingsBool optimize_rewrite_sum_if_to_count_if;
}

namespace
{

class SumIfToCountIfVisitor : public InDepthQueryTreeVisitorWithContext<SumIfToCountIfVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<SumIfToCountIfVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_sum_if_to_count_if])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || !function_node->getResultType()->equals(DataTypeUInt64()))
            return;

        auto function_name = function_node->getFunctionName();
        auto lower_function_name = Poco::toLower(function_name);

        /// sumIf, SumIf or sUMIf are valid function names, but sumIF or sumiF are not
        if (lower_function_name != "sum" && (lower_function_name != "sumif" || !function_name.ends_with("If")))
            return;

        auto & function_node_arguments_nodes = function_node->getArguments().getNodes();

        /// Rewrite `sumIf(1, cond)` into `countIf(cond)`
        if (lower_function_name == "sumif")
        {
            if (function_node_arguments_nodes.size() != 2)
                return;

            const auto * constant_node = function_node_arguments_nodes[0]->as<ConstantNode>();
            if (!constant_node)
                return;

            if (auto constant_type = constant_node->getResultType(); !isNativeInteger(constant_type))
                return;

            const auto & constant_value_literal = constant_node->getValue();
            if (getSettings()[Setting::aggregate_functions_null_for_empty])
                return;

            /// Rewrite `sumIf(1, cond)` into `countIf(cond)`
            auto multiplier_node = function_node_arguments_nodes[0];
            function_node_arguments_nodes[0] = std::move(function_node_arguments_nodes[1]);
            function_node_arguments_nodes.resize(1);

            resolveAggregateFunctionNodeByName(*function_node, "countIf");

            if (constant_value_literal.safeGet<UInt64>() != 1)
            {
                /// Rewrite `sumIf(123, cond)` into `123 * countIf(cond)`
                node = getMultiplyFunction(std::move(multiplier_node), node);
            }
            return;
        }

        /** Rewrite `sum(if(cond, 1, 0))` into `countIf(cond)`.
          * Rewrite `sum(if(cond, 0, 1))` into `countIf(not(cond))`.
          */
        if (function_node_arguments_nodes.size() != 1)
            return;

        auto & nested_argument = function_node_arguments_nodes[0];
        auto * nested_function = nested_argument->as<FunctionNode>();
        if (!nested_function || nested_function->getFunctionName() != "if")
            return;

        const auto nested_if_function_arguments_nodes = nested_function->getArguments().getNodes();
        if (nested_if_function_arguments_nodes.size() != 3)
            return;

        const auto & cond_argument = nested_if_function_arguments_nodes[0];
        const auto * if_true_condition_constant_node = nested_if_function_arguments_nodes[1]->as<ConstantNode>();
        const auto * if_false_condition_constant_node = nested_if_function_arguments_nodes[2]->as<ConstantNode>();

        if (!if_true_condition_constant_node || !if_false_condition_constant_node)
            return;

        if (auto constant_type = if_true_condition_constant_node->getResultType(); !isNativeInteger(constant_type))
            return;

        if (auto constant_type = if_false_condition_constant_node->getResultType(); !isNativeInteger(constant_type))
            return;

        const auto & if_true_condition_constant_value_literal = if_true_condition_constant_node->getValue();
        const auto & if_false_condition_constant_value_literal = if_false_condition_constant_node->getValue();

        auto if_true_condition_value = if_true_condition_constant_value_literal.safeGet<UInt64>();
        auto if_false_condition_value = if_false_condition_constant_value_literal.safeGet<UInt64>();

        if (if_false_condition_value == 0)
        {
            /// Rewrite `sum(if(cond, 1, 0))` into `countIf(cond)`.
            function_node_arguments_nodes[0] = nested_if_function_arguments_nodes[0];
            function_node_arguments_nodes.resize(1);

            resolveAggregateFunctionNodeByName(*function_node, "countIf");

            if (if_true_condition_value != 1)
            {
                /// Rewrite `sum(if(cond, 123, 0))` into `123 * countIf(cond)`.
                node = getMultiplyFunction(nested_if_function_arguments_nodes[1], node);
            }
            return;
        }

        if (if_true_condition_value == 0 && !cond_argument->getResultType()->isNullable())
        {
            /// Rewrite `sum(if(cond, 0, 1))` into `countIf(not(cond))` if condition is not Nullable (otherwise the result can be different).
            DataTypePtr not_function_result_type = std::make_shared<DataTypeUInt8>();

            const auto & condition_result_type = nested_if_function_arguments_nodes[0]->getResultType();
            if (condition_result_type->isNullable())
                not_function_result_type = makeNullable(not_function_result_type);

            auto not_function = std::make_shared<FunctionNode>("not");

            auto & not_function_arguments = not_function->getArguments().getNodes();
            not_function_arguments.push_back(nested_if_function_arguments_nodes[0]);

            not_function->resolveAsFunction(FunctionFactory::instance().get("not", getContext())->build(not_function->getArgumentColumns()));

            function_node_arguments_nodes[0] = std::move(not_function);
            function_node_arguments_nodes.resize(1);

            resolveAggregateFunctionNodeByName(*function_node, "countIf");

            if (if_false_condition_value != 1)
            {
                /// Rewrite `sum(if(cond, 0, 123))` into `123 * countIf(not(cond))` if condition is not Nullable (otherwise the result can be different).
                node = getMultiplyFunction(nested_if_function_arguments_nodes[2], node);
            }
            return;
        }
    }

private:
    QueryTreeNodePtr getMultiplyFunction(QueryTreeNodePtr left, QueryTreeNodePtr right)
    {
        auto multiply_function_node = std::make_shared<FunctionNode>("multiply");
        auto & multiply_arguments_nodes = multiply_function_node->getArguments().getNodes();
        multiply_arguments_nodes.push_back(std::move(left));
        multiply_arguments_nodes.push_back(std::move(right));

        auto multiply_function_base = FunctionFactory::instance().get("multiply", getContext())->build(multiply_function_node->getArgumentColumns());
        multiply_function_node->resolveAsFunction(std::move(multiply_function_base));
        return std::move(multiply_function_node);
    }
};

}

void SumIfToCountIfPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    SumIfToCountIfVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
