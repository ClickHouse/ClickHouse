#include <QueryCoordination/Optimizer/Tasks/Scheduler.h>
#include <Common/logger_useful.h>

namespace DB
{

void Scheduler::run()
{
    while (!stack.empty())
    {
        auto task = std::move(stack.top());
        stack.pop();

        LOG_TRACE(&Poco::Logger::get("Scheduler"), "Pop: {}", task->getDescription());

        task->execute();
    }
}

void Scheduler::pushTask(OptimizeTaskPtr task)
{
    LOG_TRACE(&Poco::Logger::get("Scheduler"), "Push: {}", task->getDescription());
    stack.push(std::move(task));
}

}
