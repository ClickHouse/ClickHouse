#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/ConvertInToEqualPass.h>
#include <Functions/equals.h>
#include <Functions/notEquals.h>

namespace DB
{

class ConvertInToEqualPassVisitor : public InDepthQueryTreeVisitorWithContext<ConvertInToEqualPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertInToEqualPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        static const std::unordered_map<String, String> MAPPING = {
            {"in", "equals"},
            {"notIn", "notEquals"}
        };
        auto * func_node = node->as<FunctionNode>();
        if (!func_node
            || !MAPPING.contains(func_node->getFunctionName())
            || func_node->getArguments().getNodes().size() != 2)
            return ;
        auto args = func_node->getArguments().getNodes();
        auto * column_node = args[0]->as<ColumnNode>();
        auto * constant_node = args[1]->as<ConstantNode>();
        if (!column_node || !constant_node)
            return ;
        // IN multiple values is not supported
        if (constant_node->getValue().getType() == Field::Types::Which::Tuple
            || constant_node->getValue().getType() == Field::Types::Which::Array)
            return ;
        // x IN null not equivalent to x = null
        if (constant_node->hasSourceExpression() || constant_node->getValue().isNull())
            return ;
        auto result_func_name = MAPPING.at(func_node->getFunctionName());
        auto equal = std::make_shared<FunctionNode>(result_func_name);
        QueryTreeNodes arguments{column_node->clone(), constant_node->clone()};
        equal->getArguments().getNodes() = std::move(arguments);
        FunctionOverloadResolverPtr resolver;
        bool decimal_check_overflow = getContext()->getSettingsRef().decimal_check_overflow;
        if (result_func_name == "equals")
        {
            resolver = createInternalFunctionEqualOverloadResolver(decimal_check_overflow);
        }
        else
        {
            resolver = createInternalFunctionNotEqualOverloadResolver(decimal_check_overflow);
        }
        try
        {
            equal->resolveAsFunction(resolver);
        }
        catch (...)
        {
            // When function resolver fails, we should not replace the function node
            return;
        }
        node = equal;
    }
};

void ConvertInToEqualPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ConvertInToEqualPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}
}
