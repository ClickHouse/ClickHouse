#include <Analyzer/Passes/OptimizeLimitByInjectiveFunctionsPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool optimize_injective_functions_in_limit_by;
}

namespace
{

class OptimizeLimitByInjectiveFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeLimitByInjectiveFunctionsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeLimitByInjectiveFunctionsVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_injective_functions_in_limit_by])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasLimitBy())
            return;

        auto & limit_by = query->getLimitBy().getNodes();

        auto new_limit_by = unwrapInjectiveFunctionsInKeys(limit_by, false);

        /// Atleast one key is needed for LIMIT BY.
        if (!new_limit_by.empty())
            limit_by = std::move(new_limit_by);
    }
};

}

void OptimizeLimitByInjectiveFunctionsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeLimitByInjectiveFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
