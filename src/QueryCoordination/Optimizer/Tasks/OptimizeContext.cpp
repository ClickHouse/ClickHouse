#include <QueryCoordination/Optimizer/Tasks/OptimizeContext.h>
#include <QueryCoordination/Optimizer/Tasks/Scheduler.h>

namespace DB
{

OptimizeContext::OptimizeContext(Memo & memo_, Scheduler & scheduler_, ContextPtr query_context_)
    : memo(memo_), scheduler(scheduler_), query_context(query_context_), cbo_settings(CBOSettings::fromContext(query_context_))
{
}

Memo & OptimizeContext::getMemo()
{
    return memo;
}

ContextPtr OptimizeContext::getQueryContext() const
{
    return query_context;
}

const CBOSettings & OptimizeContext::getCBOSettings() const
{
    return cbo_settings;
}

void OptimizeContext::pushTask(OptimizeTaskPtr task)
{
    scheduler.pushTask(std::move(task));
}

}
