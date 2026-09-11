#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

namespace DB
{

std::unique_lock<std::mutex> ProcessorLock::lockRound()
{
    return std::unique_lock(mutex);
}

ProcessorLock::Status ProcessorLock::status() const
{
    return value.load();
}

void ProcessorLock::setExecuting()
{
    value.store(Status::Executing);
}

void ProcessorLock::setIdle()
{
    value.store(Status::Idle);
}

void ProcessorLock::finish()
{
    value.store(Status::Finished);
}

bool ProcessorLock::isFinished() const
{
    return value.load() == Status::Finished;
}

}
