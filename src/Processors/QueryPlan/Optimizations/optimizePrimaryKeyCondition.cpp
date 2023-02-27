#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/StorageMerge.h>
#include <Interpreters/ActionsDAG.h>
#include <deque>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyCondition(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    auto * read_from_merge = typeid_cast<ReadFromMerge *>(frame.node->step.get());

    if (!read_from_merge && !read_from_merge_tree)
        return;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            if (read_from_merge_tree)
                read_from_merge_tree->addFilter(filter_step->getExpression(), filter_step->getFilterColumnName());
            if (read_from_merge)
                read_from_merge->addFilter(filter_step->getExpression(), filter_step->getFilterColumnName());
        }
        /// Note: actually, plan optimizations merge Filter and Expression steps.
        /// Ideally, chain should look like (Expression -> ...) -> (Filter -> ...) -> ReadFromStorage,
        /// So this is likely not needed.
        else if (typeid_cast<ExpressionStep *>(iter->node->step.get()))
            continue;
        else
            break;
    }
}

}
