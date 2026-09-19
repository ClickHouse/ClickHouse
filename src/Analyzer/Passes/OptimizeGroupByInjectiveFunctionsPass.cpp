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

        /// Skip when a GROUP BY modifier produces rows where a grouping key is absent from the set
        /// being aggregated: CUBE/ROLLUP subtotals, GROUPING SETS non-member sets, and the WITH
        /// TOTALS row. In such a row the key is output as its column default. Rewriting f(g) -> g
        /// makes the output projection recompute f(defaultOf(g)) instead of defaultOf(typeOf(f(g))),
        /// which changes the result. See #110715.
        if (query->isGroupByWithCube() || query->isGroupByWithRollup()
            || query->isGroupByWithGroupingSets() || query->isGroupByWithTotals())
            return;

        bool allow_suspicious_types = getSettings()[Setting::allow_suspicious_types_in_group_by];

        auto & group_by = query->getGroupBy().getNodes();
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
