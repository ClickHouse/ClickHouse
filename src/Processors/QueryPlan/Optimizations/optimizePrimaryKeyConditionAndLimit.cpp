#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilter *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    if (storage_prewhere_info)
    {
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);
        if (storage_prewhere_info->row_level_filter)
            source_step_with_filter->addFilter(storage_prewhere_info->row_level_filter->clone(), storage_prewhere_info->row_level_column_name);
    }

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            source_step_with_filter->addFilter(filter_step->getExpression().clone(), filter_step->getFilterColumnName());
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            source_step_with_filter->setLimit(limit_step->getLimitForSorting());
            break;
        }
        else if (typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            /// Note: actually, plan optimizations merge Filter and Expression steps.
            /// Ideally, chain should look like (Expression -> ...) -> (Filter -> ...) -> ReadFromStorage,
            /// So this is likely not needed.
            continue;
        }
        else
        {
            break;
        }
    }

    source_step_with_filter->applyFilters();
}

}
