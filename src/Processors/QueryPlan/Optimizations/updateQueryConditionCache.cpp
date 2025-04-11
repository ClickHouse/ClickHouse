#include <memory>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include "Interpreters/Cache/QueryConditionCache.h"

namespace DB::QueryPlanOptimizations
{

/// This is not really an optimization. The purpose of this function is to extract and hash the filter condition of WHERE or PREWHERE
/// filters. These correspond to these steps:
///
///   [...]
///     ^
///     |
///     |
///   FilterStep
///     ^
///     |
///     |
///   ReadFromMergeTree
///
/// Later on, the hashed filter condition will be used as a key in the query condition cache.
///
void updateQueryConditionCache(const Stack & stack, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.use_query_condition_cache)
        return;

    const auto & frame = stack.back();

    auto * read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree)
        return;

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto & filter_actions_dag = query_info.filter_actions_dag;
    if (!filter_actions_dag || query_info.isFinal())
        return;

    const auto & outputs = filter_actions_dag->getOutputs();
    for (const auto * output : outputs)
        if (!VirtualColumnUtils::isDeterministic(output))
            return;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            size_t condition_hash = filter_actions_dag->getOutputs().front()->getHash();
            auto query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
            auto query_condition_cache_writer = std::make_shared<QueryConditionCacheWriter>(
                query_condition_cache,
                optimization_settings.query_condition_cache_zero_ratio_threshold,
                condition_hash);

            filter_step->setQueryConditionCacheWriter(query_condition_cache_writer);
            return;
        }
    }
}

}
