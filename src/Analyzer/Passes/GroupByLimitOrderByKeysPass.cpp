#include <Analyzer/Passes/GroupByLimitOrderByKeysPass.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 automatic_parallel_replicas_mode;
    extern const SettingsBool enable_group_by_top_k_optimization;
    extern const SettingsUInt64 max_rows_to_group_by;
}

void GroupByLimitOrderByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /*context*/)
{
    auto * query = query_tree_node->as<QueryNode>();
    if (!query)
        return;

    /// The single-node plan of automatic parallel replicas is built with parallel replicas disabled, and must be ordered too.
    const auto & query_context = query->getContext();
    if (!query_context->canUseParallelReplicasOnInitiator()
        && query_context->getSettingsRef()[Setting::automatic_parallel_replicas_mode] == 0)
        return;

    /// The heap refuses `max_rows_to_group_by`, which `OptimizeTrivialGroupByLimitPass` may have just set.
    const auto & settings = query_context->getSettingsRef();
    if (!settings[Setting::enable_group_by_top_k_optimization] || settings[Setting::max_rows_to_group_by] != 0)
        return;

    /// The shapes the heap refuses either way, see `applyTopKPushdownToPartialAggregation`.
    if (!query->hasGroupBy() || !query->hasLimit() || query->hasOrderBy() || query->hasLimitBy() || query->isDistinct()
        || query->hasHaving() || query->hasWindow() || query->hasQualify() || query->isGroupByWithTotals()
        || query->isGroupByWithRollup() || query->isGroupByWithCube() || query->isGroupByWithGroupingSets())
        return;

    auto & order_by = query->getOrderBy().getNodes();
    for (const auto & key : query->getGroupBy().getNodes())
        order_by.push_back(std::make_shared<SortNode>(key->clone()));
}

}
