#include <Analyzer/Passes/SumIfToCountIfPass.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace
{

class SumIfToCountIfVisitor : public InDepthQueryTreeVisitor<SumIfToCountIfVisitor>
{
public:
    explicit SumIfToCountIfVisitor(ContextPtr & context_)
        : context(context_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction())
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

            const auto & constant_value_literal = constant_node->getValue();
            if (!isInt64OrUInt64FieldType(constant_value_literal.getType()))
                return;

            if (constant_value_literal.get<UInt64>() != 1 || context->getSettingsRef().aggregate_functions_null_for_empty)
                return;

            function_node_arguments_nodes[0] = std::move(function_node_arguments_nodes[1]);
            function_node_arguments_nodes.resize(1);

            resolveAsCountIfAggregateFunction(*function_node, function_node_arguments_nodes[0]->getResultType());
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

        const auto & nested_if_function_arguments_nodes = nested_function->getArguments().getNodes();
        if (nested_if_function_arguments_nodes.size() != 3)
            return;

        const auto & cond_argument = nested_if_function_arguments_nodes[0];
        const auto * if_true_condition_constant_node = nested_if_function_arguments_nodes[1]->as<ConstantNode>();
        const auto * if_false_condition_constant_node = nested_if_function_arguments_nodes[2]->as<ConstantNode>();

        if (!if_true_condition_constant_node || !if_false_condition_constant_node)
            return;

        const auto & if_true_condition_constant_value_literal = if_true_condition_constant_node->getValue();
        const auto & if_false_condition_constant_value_literal = if_false_condition_constant_node->getValue();

        if (!isInt64OrUInt64FieldType(if_true_condition_constant_value_literal.getType()) ||
            !isInt64OrUInt64FieldType(if_false_condition_constant_value_literal.getType()))
            return;

        auto if_true_condition_value = if_true_condition_constant_value_literal.get<UInt64>();
        auto if_false_condition_value = if_false_condition_constant_value_literal.get<UInt64>();

        /// Rewrite `sum(if(cond, 1, 0))` into `countIf(cond)`.
        if (if_true_condition_value == 1 && if_false_condition_value == 0)
        {
            function_node_arguments_nodes[0] = nested_if_function_arguments_nodes[0];
            function_node_arguments_nodes.resize(1);

            resolveAsCountIfAggregateFunction(*function_node, function_node_arguments_nodes[0]->getResultType());
            return;
        }

        /// Rewrite `sum(if(cond, 0, 1))` into `countIf(not(cond))` if condition is not Nullable (otherwise the result can be different).
        if (if_true_condition_value == 0 && if_false_condition_value == 1 && !cond_argument->getResultType()->isNullable())
        {
            DataTypePtr not_function_result_type = std::make_shared<DataTypeUInt8>();

            const auto & condition_result_type = nested_if_function_arguments_nodes[0]->getResultType();
            if (condition_result_type->isNullable())
                not_function_result_type = makeNullable(not_function_result_type);

            auto not_function = std::make_shared<FunctionNode>("not");

            auto & not_function_arguments = not_function->getArguments().getNodes();
            not_function_arguments.push_back(nested_if_function_arguments_nodes[0]);

            not_function->resolveAsFunction(FunctionFactory::instance().get("not", context)->build(not_function->getArgumentColumns()));

            function_node_arguments_nodes[0] = std::move(not_function);
            function_node_arguments_nodes.resize(1);

            resolveAsCountIfAggregateFunction(*function_node, function_node_arguments_nodes[0]->getResultType());
            return;
        }
    }

private:
    static inline void resolveAsCountIfAggregateFunction(FunctionNode & function_node, const DataTypePtr & argument_type)
    {
        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get("countIf",
            {argument_type},
            function_node.getAggregateFunction()->getParameters(),
            properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function));
    }

    ContextPtr & context;
};

}

void SumIfToCountIfPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    SumIfToCountIfVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
