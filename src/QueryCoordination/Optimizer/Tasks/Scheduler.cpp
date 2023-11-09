#include <QueryCoordination/Optimizer/Tasks/Scheduler.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CBO_OPTIMIZATION_TIMEOUT;
}

void Scheduler::run()
{
    Poco::Timestamp now;
    auto diff = static_cast<UInt64>(now - start_time_ms);

    if (diff / 1000 > max_run_time_ms)
        throw Exception(ErrorCodes::CBO_OPTIMIZATION_TIMEOUT, "CBO optimization runs longer than {}ms", max_run_time_ms);

    while (!stack.empty())
    {
        auto task = std::move(stack.top());
        stack.pop();

        LOG_TEST(&Poco::Logger::get("Scheduler"), "Pop: {}", task->getDescription());
        task->execute();
    }
}

void Scheduler::pushTask(OptimizeTaskPtr task)
{
    LOG_TEST(&Poco::Logger::get("Scheduler"), "Push: {}", task->getDescription());
    stack.push(std::move(task));
}

}
