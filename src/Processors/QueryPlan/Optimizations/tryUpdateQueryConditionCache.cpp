#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

namespace DB::QueryPlanOptimizations
{

void tryUpdateQueryConditionCache(const QueryPlanOptimizationSettings & optimization_settings, const Stack & stack)
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

    if (!VirtualColumnUtils::isDeterministic(filter_actions_dag->getOutputs().front())) /// TODO check if front() still works for >1 condition
        return;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            size_t condition_hash = filter_actions_dag->getOutputs().front()->getHash();
            filter_step->setQueryConditionKey(condition_hash);
            return;
        }
    }
}

}
