#include <QueryCoordination/Optimizer/Tasks/OptimizeTask.h>

namespace DB
{

OptimizeTask::OptimizeTask(TaskContextPtr task_context_) : task_context(task_context_)
{
}

OptimizeTask::~OptimizeTask() = default;

void OptimizeTask::pushTask(OptimizeTaskPtr task)
{
    task_context->pushTask(std::move(task));
}

ContextPtr OptimizeTask::getQueryContext()
{
    return task_context->getQueryContext();
}

}
