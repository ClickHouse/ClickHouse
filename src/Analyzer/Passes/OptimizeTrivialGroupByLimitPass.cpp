#include <Analyzer/Passes/OptimizeTrivialGroupByLimitPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TrivialGroupByLimit.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
}

void OptimizeTrivialGroupByLimitPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto * query = query_tree_node->as<QueryNode>();
    if (!query || !query->hasGroupBy() || !query->hasLimit() || query->hasHaving() || query->hasOrderBy() || query->hasWindow()
        || query->hasQualify() || query->hasLimitBy() || query->isDistinct() || query->isGroupByWithTotals()
        || query->isGroupByWithRollup() || query->isGroupByWithCube() || query->isGroupByWithGroupingSets()
        || hasAggregateFunctionNodes(query->getProjectionNode()))
        return;

    /// The window-function and `arrayJoin` projection guards live in `getTrivialGroupByLimit`,
    /// shared with the aggregate cutoff of the planner.
    const Settings & settings = context->getSettingsRef();
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

    auto & mutable_context = query->getMutableContext();
    mutable_context->setSetting("max_rows_to_group_by", *max_rows);
    if (settings[Setting::group_by_overflow_mode] != OverflowMode::ANY)
        mutable_context->setSetting("group_by_overflow_mode", Field("any"));
}

}
