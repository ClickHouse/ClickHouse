#include <Analyzer/Passes/OptimizeLimitByFunctionKeysPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_limit_by_function_keys;
}

namespace
{

class OptimizeLimitByFunctionKeysVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeLimitByFunctionKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeLimitByFunctionKeysVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_limit_by_function_keys])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasLimitBy())
            return;

        removeKeysThatAreFunctionsOfOtherKeys(query->getLimitBy().getNodes());
    }
};

}

void OptimizeLimitByFunctionKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeLimitByFunctionKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
