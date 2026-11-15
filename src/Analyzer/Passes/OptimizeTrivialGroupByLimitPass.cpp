#include <Analyzer/Passes/OptimizeTrivialGroupByLimitPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TrivialGroupByLimit.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_group_by;
}

void OptimizeTrivialGroupByLimitPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /* context */)
{
    auto * query = query_tree_node->as<QueryNode>();
    if (!query || hasAggregateFunctionNodes(query->getProjectionNode()))
        return;

    const Settings & settings = query->getContext()->getSettingsRef();
    auto max_rows = getTrivialGroupByLimit(*query, settings);
    if (!max_rows)
        return;

    /// If the user has already set `max_rows_to_group_by`, we only apply the optimization
    /// when our derived value is strictly smaller — otherwise the user's setting is tighter
    /// and ours would be a no-op. When the user has a tighter throw/break contract, we'd
    /// also break their semantics (already guarded inside `getTrivialGroupByLimit`).
    const UInt64 user_max_rows = settings[Setting::max_rows_to_group_by];
    if (user_max_rows != 0 && user_max_rows <= *max_rows)
        return;

    /// Do not set `max_rows_to_group_by` here: views and subqueries on shards inherit the settings,
    /// and their aggregations must stay complete.
    query->getMutableContext()->setSetting("trivial_group_by_limit", *max_rows);
}

}
