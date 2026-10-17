#include <Analyzer/Passes/GroupByLimitOrderByKeysPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Core/Settings.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 automatic_parallel_replicas_mode;
    extern const SettingsBool enable_group_by_top_k_optimization;
    extern const SettingsBool exact_rows_before_limit;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsUInt64 query_plan_max_limit_for_top_k_optimization;
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
    if (!settings[Setting::enable_group_by_top_k_optimization] || settings[Setting::max_rows_to_group_by] != 0
        || settings[Setting::exact_rows_before_limit])
        return;

    /// The shapes the heap refuses either way, see `applyTopKPushdownToPartialAggregation`.
    if (!query->hasGroupBy() || !query->hasLimit() || query->hasOrderBy() || query->hasLimitBy() || query->isDistinct()
        || query->hasHaving() || query->hasWindow() || query->hasQualify() || query->isGroupByWithTotals()
        || query->isGroupByWithRollup() || query->isGroupByWithCube() || query->isGroupByWithGroupingSets()
        || query->isLimitWithTies() || query->hasLimitAfter() || query->hasLimitUntil())
        return;

    /// Negative and fractional values are kept as `Int64` and `Float64`, and the heap refuses them.
    const auto get_uint = [](const QueryTreeNodePtr & node) -> std::optional<UInt64>
    {
        const auto * constant = node->as<ConstantNode>();
        if (!constant || constant->getValue().getType() != Field::Types::UInt64)
            return std::nullopt;
        return constant->getValue().safeGet<UInt64>();
    };

    /// Without the heap, the order would only cost a sort; the planner refuses the same limits.
    const auto limit = get_uint(query->getLimit());
    const auto offset = query->hasOffset() ? get_uint(query->getOffset()) : std::optional<UInt64>(0);
    UInt64 heap_size = 0;
    if (!limit || !offset || common::addOverflow(*limit, *offset, heap_size) || heap_size == 0
        || heap_size > Aggregator::Params::TopKParams::max_k
        || (settings[Setting::query_plan_max_limit_for_top_k_optimization] != 0
            && heap_size > settings[Setting::query_plan_max_limit_for_top_k_optimization]))
        return;

    auto & order_by = query->getOrderBy().getNodes();
    for (const auto & key : query->getGroupBy().getNodes())
        order_by.push_back(std::make_shared<SortNode>(key->clone()));
}

}
