#include <Analyzer/MultiIfToIfPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class MultiIfToIfVisitor : public InDepthQueryTreeVisitor<MultiIfToIfVisitor>
{
public:
    explicit MultiIfToIfVisitor(FunctionOverloadResolverPtr if_function_ptr_)
        : if_function_ptr(if_function_ptr_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "multiIf")
            return;

        if (function_node->getArguments().getNodes().size() != 3)
            return;

        auto result_type = function_node->getResultType();
        function_node->resolveAsFunction(if_function_ptr, result_type);
    }

private:
    FunctionOverloadResolverPtr if_function_ptr;
};

}

void MultiIfToIfPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    MultiIfToIfVisitor visitor(FunctionFactory::instance().get("if", context));
    visitor.visit(query_tree_node);
}

}
