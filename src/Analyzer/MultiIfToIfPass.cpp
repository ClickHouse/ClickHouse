#include <Analyzer/MultiIfToIfPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class MultiIfToIfVisitorMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<MultiIfToIfVisitorMatcher, true>;

    struct Data
    {
        FunctionOverloadResolverPtr if_function_overload_resolver;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "multiIf")
            return;

        if (function_node->getArguments().getNodes().size() != 3)
            return;

        auto result_type = function_node->getResultType();
        function_node->resolveAsFunction(data.if_function_overload_resolver, result_type);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &)
    {
        return true;
    }
};

}

void MultiIfToIfPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    MultiIfToIfVisitorMatcher::Data data{FunctionFactory::instance().get("if", context)};
    MultiIfToIfVisitorMatcher::Visitor visitor(data);
    visitor.visit(query_tree_node);
}

}
