#include <limits>
#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include "Processors/QueryPlan/Optimizations/Cascades/Task.h"
#include "Processors/QueryPlan/QueryPlan.h"
#include "Common/logger_useful.h"

namespace DB
{

namespace QueryPlanOptimizations
{
String dumpQueryPlanShort(const QueryPlan & query_plan);
}

void CascadesOptimizer::optimize(QueryPlan & query_plan)
{
    OptimizerContext optimizer_context;
    /// Add root group
    auto root_group_id = optimizer_context.addGroup(*query_plan.getRootNode());

    LOG_TRACE(optimizer_context.log, "Initial memo:\n{}", optimizer_context.memo.dump());

    /// Add task to optimize root group
    CostLimit initial_cost_limit = std::numeric_limits<Int64>::max();
    optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(root_group_id, initial_cost_limit));

    /// Limit the time in terms of optimization tasks instead of wall clock time. This is done for stability of generated plans.
    /// Clever guys from MS SQL Server describe this in Andy Pavlo's seminar (https://www.youtube.com/watch?v=pQe1LQJiXN0)
    const size_t executed_tasks_limit = 100000;
    size_t executed_tasks_count = 0;
    for (; !optimizer_context.tasks.empty() && executed_tasks_count < executed_tasks_limit; ++executed_tasks_count)
    {
        auto task = optimizer_context.tasks.top();
        optimizer_context.tasks.pop();
        task->execute(optimizer_context);
    }

    /// Get the best plan for the root group 
    optimizer_context.getBestPlan(root_group_id);

    LOG_TRACE(optimizer_context.log, "Executed {} tasks, Memo after:\n{}", executed_tasks_count, optimizer_context.memo.dump());
    LOG_TRACE(optimizer_context.log, "Optimized plan:\n{}", QueryPlanOptimizations::dumpQueryPlanShort(query_plan));
}

}
