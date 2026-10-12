#include <Analyzer/Passes/IfConstantConditionPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
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
        if (auto collapsed_node = tryCollapseConstantConditionFunction(node))
            node = std::move(collapsed_node);
    }
};

}

void IfConstantConditionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    IfConstantConditionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
