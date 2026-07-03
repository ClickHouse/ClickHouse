#include <Analyzer/Passes/OptimizeGroupByInjectiveFunctionsPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_injective_functions_in_group_by;
    extern const SettingsBool allow_suspicious_types_in_group_by;
}

namespace
{

class OptimizeGroupByInjectiveFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>;
public:
    explicit OptimizeGroupByInjectiveFunctionsVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_injective_functions_in_group_by])
            return;

        /// Don't optimize injective functions when group_by_use_nulls=true,
        /// because in this case we make initial group by keys Nullable
        /// and eliminating some functions can cause issues with arguments Nullability
        /// during their execution. See examples in https://github.com/ClickHouse/ClickHouse/pull/61567#issuecomment-2008181143
        if (getSettings()[Setting::group_by_use_nulls])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy())
            return;

        if (query->isGroupByWithCube() || query->isGroupByWithRollup())
            return;

        bool allow_suspicious_types = getSettings()[Setting::allow_suspicious_types_in_group_by];

        auto & group_by = query->getGroupBy().getNodes();
        if (query->isGroupByWithGroupingSets())
        {
            for (auto & set : group_by)
            {
                auto & grouping_set = set->as<ListNode>()->getNodes();
                grouping_set = unwrapInjectiveFunctionsInKeys(grouping_set, allow_suspicious_types);
            }
        }
        else
            group_by = unwrapInjectiveFunctionsInKeys(group_by, allow_suspicious_types);
    }
};

}

void OptimizeGroupByInjectiveFunctionsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeGroupByInjectiveFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
