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

        if (query->isGroupByWithCube() || query->isGroupByWithRollup())
            return;

        auto & group_by = query->getGroupBy().getNodes();
        if (query->isGroupByWithGroupingSets())
        {
            for (auto & set : group_by)
            {
                auto & grouping_set = set->as<ListNode>()->getNodes();
                removeKeysThatAreFunctionsOfOtherKeys(grouping_set);
            }
        }
        else
            removeKeysThatAreFunctionsOfOtherKeys(group_by);
    }
};

void OptimizeGroupByFunctionKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeGroupByFunctionKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
