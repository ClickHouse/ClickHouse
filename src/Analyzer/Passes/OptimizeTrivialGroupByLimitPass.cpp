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
    if (!query)
        return;

    /// With aggregate functions in the projection, the settings-based rewrite would be unsound:
    /// `max_rows_to_group_by` with `group_by_overflow_mode = 'any'` is enforced per aggregation
    /// stream, so a key kept by one stream and rejected by another loses the other stream's rows
    /// and comes out of the merge with an undercounted aggregate value. That case is handled by
    /// the planner instead (see `addAggregationStep`), which enables the shared kept-keys cutoff
    /// in the `Aggregator` for local single-stage aggregation, keeping the values exact.
    if (hasAggregateFunctionNodes(query->getProjectionNode()))
        return;

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
