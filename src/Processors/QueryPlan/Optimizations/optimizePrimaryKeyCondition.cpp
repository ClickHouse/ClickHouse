#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <deque>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyCondition(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilter *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
            source_step_with_filter->addFilter(filter_step->getExpression(), filter_step->getFilterColumnName());

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
