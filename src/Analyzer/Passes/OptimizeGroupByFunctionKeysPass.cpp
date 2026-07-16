#include <Analyzer/Passes/OptimizeGroupByFunctionKeysPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_group_by_function_keys;
}

class OptimizeGroupByFunctionKeysVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByFunctionKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByFunctionKeysVisitor>;
    using Base::Base;

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (parent->getNodeType() == QueryTreeNodeType::TABLE_FUNCTION)
            return false;

        return !child->as<FunctionNode>();
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_group_by_function_keys])
            return;

        /// When group_by_use_nulls = 1 removing keys from GROUP BY can lead
        /// to unexpected types in some functions.
        /// See example in https://github.com/ClickHouse/ClickHouse/pull/61567#issuecomment-2018007887
        if (getSettings()[Setting::group_by_use_nulls])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy())
            return;

        /// Skip when a GROUP BY modifier produces rows where a grouping key is absent from the set
        /// being aggregated: CUBE/ROLLUP subtotals, GROUPING SETS non-member sets, and the WITH
        /// TOTALS row. In such a row the key is output as its column default. Dropping a key that is
        /// a function of other keys makes the output projection recompute it from those keys' totals
        /// defaults (e.g. toString(number) from number = 0 gives '0' instead of the required String
        /// default ''), which changes the result. See #110715.
        if (query->isGroupByWithCube() || query->isGroupByWithRollup()
            || query->isGroupByWithGroupingSets() || query->isGroupByWithTotals())
            return;

        auto & group_by = query->getGroupBy().getNodes();
        removeKeysThatAreFunctionsOfOtherKeys(group_by);
    }
};

void OptimizeGroupByFunctionKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeGroupByFunctionKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
