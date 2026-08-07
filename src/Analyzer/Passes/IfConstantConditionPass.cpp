#include <Analyzer/Passes/IfConstantConditionPass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace
{

class IfConstantConditionVisitor : public InDepthQueryTreeVisitorWithContext<IfConstantConditionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<IfConstantConditionVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || (function_node->getFunctionName() != "if" && function_node->getFunctionName() != "multiIf"))
            return;

        if (function_node->getArguments().getNodes().size() != 3)
            return;

        auto & first_argument = function_node->getArguments().getNodes()[0];
        const auto * first_argument_constant_node = first_argument->as<ConstantNode>();
        if (!first_argument_constant_node)
            return;

        const auto & condition_value = first_argument_constant_node->getValue();

        bool condition_boolean_value = false;

        if (condition_value.getType() == Field::Types::Int64)
            condition_boolean_value = static_cast<bool>(condition_value.safeGet<Int64>());
        else if (condition_value.getType() == Field::Types::UInt64)
            condition_boolean_value = static_cast<bool>(condition_value.safeGet<UInt64>());
        else
            return;

        QueryTreeNodePtr argument_node;
        if (condition_boolean_value)
            argument_node = function_node->getArguments().getNodes()[1];
        else
            argument_node = function_node->getArguments().getNodes()[2];

        if (node->getResultType()->equals(*argument_node->getResultType()))
            node = argument_node;
    }
};

}

void IfConstantConditionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    IfConstantConditionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
