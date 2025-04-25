#include <Analyzer/Passes/OptimizeTrivialGroupByLimitPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsBool optimize_trivial_group_by_limit_query;
}

void OptimizeTrivialGroupByLimitPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    if (!settings[Setting::optimize_trivial_group_by_limit_query])
        return;

    auto * query = query_tree_node->as<QueryNode>();
    if (query && query->hasGroupBy() && query->hasLimit() && !query->hasHaving() && !query->hasOrderBy() && !query->hasWindow())
    {
        auto & mutable_context = query->getMutableContext();
        if (settings[Setting::max_rows_to_group_by] == 0)
        {
            UInt64 limit = query->getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();
            UInt64 offset = 0;
            if (query->hasOffset())
                offset = query->getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
            mutable_context->setSetting("max_rows_to_group_by", limit + offset);
            mutable_context->setSetting("group_by_overflow_mode", Field("any"));
        }
    }
}

}
