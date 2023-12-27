#include <Analyzer/Passes/MultiIfToIfPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class MultiIfToIfVisitor : public InDepthQueryTreeVisitorWithContext<MultiIfToIfVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MultiIfToIfVisitor>;
    using Base::Base;

    explicit MultiIfToIfVisitor(FunctionOverloadResolverPtr if_function_ptr_, ContextPtr context)
        : Base(std::move(context))
        , if_function_ptr(std::move(if_function_ptr_))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_multiif_to_if)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "multiIf")
            return;

        if (function_node->getArguments().getNodes().size() != 3)
            return;

        auto if_function_value = if_function_ptr->build(function_node->getArgumentColumns());
        if (!if_function_value->getResultType()->equals(*function_node->getResultType()))
        {
            /** We faced some corner case, when result type of `if` and `multiIf` are different.
              * For example, currently `if(NULL`, a, b)` returns type of `a` column,
              * but multiIf(NULL, a, b) returns supertypetype of `a` and `b`.
              */
            return;
        }

        function_node->resolveAsFunction(std::move(if_function_value));
    }

private:
    FunctionOverloadResolverPtr if_function_ptr;
};

}

void MultiIfToIfPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    auto if_function_ptr = FunctionFactory::instance().get("if", context);
    MultiIfToIfVisitor visitor(std::move(if_function_ptr), std::move(context));
    visitor.visit(query_tree_node);
}

}
