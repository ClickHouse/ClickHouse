#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Optimizer.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeContext.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeGroup.h>
#include <QueryCoordination/Optimizer/Tasks/Scheduler.h>

namespace DB
{

StepTree Optimizer::optimize(QueryPlan && plan, ContextPtr query_context)
{
    /// rewrite plan by rule

    /// init Memo by plan
    Memo memo(std::move(plan), query_context);

//    /// logical equivalent transform
//    memo.transform();
//
//    memo.deriveStat();
//
//    /// enforce properties
//    memo.enforce();


    PhysicalProperties initial_prop{.distribution = {.type = PhysicalProperties::DistributionType::Singleton}};

    Scheduler scheduler;

    OptimizeContextPtr optimize_context = std::make_shared<OptimizeContext>(memo, scheduler, query_context);
    TaskContextPtr task_context = std::make_shared<TaskContext>(memo.rootGroup(), initial_prop, optimize_context);
    scheduler.pushTask(std::make_unique<OptimizeGroup>(task_context));
    scheduler.run();

    memo.dump();
    return memo.extractPlan();
}

}
