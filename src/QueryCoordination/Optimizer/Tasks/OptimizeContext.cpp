#include <QueryCoordination/Optimizer/Tasks/OptimizeContext.h>
#include <QueryCoordination/Optimizer/Tasks/Scheduler.h>

namespace DB
{

OptimizeContext::OptimizeContext(Memo & memo_, Scheduler & scheduler_, ContextPtr query_context_)
    : memo(memo_), scheduler(scheduler_), query_context(query_context_)
{
}

Memo & OptimizeContext::getMemo()
{
    return memo;
}

ContextPtr OptimizeContext::getQueryContext()
{
    return query_context;
}

void OptimizeContext::pushTask(OptimizeTaskPtr task)
{
    scheduler.pushTask(std::move(task));
}

}
