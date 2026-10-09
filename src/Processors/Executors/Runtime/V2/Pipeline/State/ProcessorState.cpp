#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorState.h>

#include <fmt/format.h>

namespace DB
{

namespace Runtime::V2
{

void ProcessorLock::setName(std::string name)
{
    Wyrd::setName(mutex, fmt::format("{}.round_mutex", name));
    Wyrd::setName(value, fmt::format("{}.status", name));
}

std::unique_lock<Wyrd::Mutex> ProcessorLock::lockRound()
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

}
