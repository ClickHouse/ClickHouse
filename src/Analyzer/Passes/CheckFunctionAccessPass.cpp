#include <Analyzer/Passes/CheckFunctionAccessPass.h>

#include <Access/AccessControl.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Interpreters/checkFunctionAccess.h>

namespace DB
{

namespace
{

class CheckFunctionAccessVisitor : public InDepthQueryTreeVisitorWithContext<CheckFunctionAccessVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CheckFunctionAccessVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        const auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isOrdinaryFunction())
            return;

        /// The name of the implementation, not of the node: the function factory may give another function
        /// than the requested one, e.g. `toTimeWithFixedDate` for `toTime` under `use_legacy_to_time`.
        if (const auto function = function_node->getFunction())
            checkFunctionAccess(getContext(), function->getName());
    }
};

}

void CheckFunctionAccessPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (!AccessControl::hasFunctionsRequiringGrant())
        return;

    CheckFunctionAccessVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
