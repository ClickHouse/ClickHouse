#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB::QueryPlanOptimizations
{

void tryApplyQueryConditionCache(const QueryPlanOptimizationSettings & optimization_settings, const Stack & stack)
{
    if (!optimization_settings.enable_writes_to_query_condition_cache)
        return;

    const auto & frame = stack.back();

    auto * read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree)
        return;

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    if (!query_info.filter_actions_dag)
        return;

    auto context = read_from_merge_tree->getContext();

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            String condition = query_info.filter_actions_dag->getOutputs()[0]->result_name;
            filter_step->setQueryConditionCacheAndKey(context->getQueryConditionCache(), condition);
            break;
        }
    }
}

}
