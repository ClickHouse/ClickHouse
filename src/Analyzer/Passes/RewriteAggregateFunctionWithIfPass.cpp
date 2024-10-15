#include <Analyzer/Passes/RewriteAggregateFunctionWithIfPass.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Core/Settings.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

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

                /// We need to preserve the output type from if()
                if (if_arguments_nodes[1]->getResultType()->getName() != if_node->getResultType()->getName())
                    new_arguments[0] = createCastFunction(std::move(if_arguments_nodes[1]), if_node->getResultType(), getContext());
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

                if (if_arguments_nodes[2]->getResultType()->getName() != if_node->getResultType()->getName())
                    new_arguments[0] = createCastFunction(std::move(if_arguments_nodes[2]), if_node->getResultType(), getContext());
                else
                    new_arguments[0] = std::move(if_arguments_nodes[2]);

                auto not_function = std::make_shared<FunctionNode>("not");
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
