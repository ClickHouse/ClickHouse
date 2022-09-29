#include <Analyzer/IfConstantConditionPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class IfConstantConditionMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<IfConstantConditionMatcher, true>;

    struct Data
    {
    };

    static void visit(QueryTreeNodePtr & node, Data &)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || (function_node->getFunctionName() != "if" && function_node->getFunctionName() != "multiIf"))
            return;

        if (function_node->getArguments().getNodes().size() != 3)
            return;

        auto & first_argument = function_node->getArguments().getNodes()[0];
        auto first_argument_constant_value = first_argument->getConstantValueOrNull();
        if (!first_argument_constant_value)
            return;

        const auto & condition_value = first_argument_constant_value->getValue();

        bool condition_boolean_value = false;

        if (condition_value.getType() == Field::Types::Int64)
            condition_boolean_value = static_cast<bool>(condition_value.safeGet<Int64>());
        else if (condition_value.getType() == Field::Types::UInt64)
            condition_boolean_value = static_cast<bool>(condition_value.safeGet<UInt64>());
        else
            return;

        if (condition_boolean_value)
            node = function_node->getArguments().getNodes()[1];
        else
            node = function_node->getArguments().getNodes()[2];
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &)
    {
        return true;
    }
};

}

void IfConstantConditionPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    IfConstantConditionMatcher::Data data{};
    IfConstantConditionMatcher::Visitor visitor(data);
    visitor.visit(query_tree_node);
}

}
