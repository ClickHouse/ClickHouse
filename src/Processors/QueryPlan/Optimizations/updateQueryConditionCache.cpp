#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

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

    /// Restrict to the case that ActionsDAG has a single output. This isn't technically necessary but de-risks
    /// the implementation a lot while not losing much usefulness.
    if (outputs.size() != 1)
        return;

    for (const auto * output : outputs)
        if (!VirtualColumnUtils::isDeterministic(output))
            return;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            size_t condition_hash = filter_actions_dag->getOutputs()[0]->getHash();

            String condition;
            if (optimization_settings.query_condition_cache_store_conditions_as_plaintext)
            {
                Names outputs_names = filter_actions_dag->getNames();
                condition = outputs_names[0];
            }

            filter_step->setConditionForQueryConditionCache(condition_hash, condition);
            return;
        }
    }
}

}
