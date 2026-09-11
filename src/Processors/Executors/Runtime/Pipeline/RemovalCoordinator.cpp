#include <Processors/Executors/Runtime/Pipeline/RemovalCoordinator.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void RemovalCoordinator::submit(Processors processors)
{
    std::lock_guard lock(mutex);

    Group & group = groups.emplace_back();
    group.processors = std::move(processors);

    for (const auto & processor : group.processors)
    {
        if (finished.erase(processor.get()))
            continue;

        if (!pending.emplace(processor.get(), &group).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} is already pending removal", processor->getName());

        ++group.not_finished;
    }

    if (group.not_finished == 0)
        ++ready_groups;
}

void RemovalCoordinator::onFinished(IProcessor * processor)
{
    std::lock_guard lock(mutex);

    auto it = pending.find(processor);
    if (it == pending.end())
    {
        finished.insert(processor);
        return;
    }

    Group & group = *it->second;
    group.not_finished -= 1;
    pending.erase(it);

    if (group.not_finished == 0)
        ++ready_groups;
}

Processors RemovalCoordinator::takeReadyForRemoval()
{
    std::lock_guard lock(mutex);

    Processors result;
    for (auto it = groups.begin(); it != groups.end();)
    {
        if (it->not_finished != 0)
        {
            ++it;
            continue;
        }

        result.splice(result.end(), it->processors);
        it = groups.erase(it);
    }

    ready_groups.store(0);
    return result;
}

bool RemovalCoordinator::hasReady() const
{
    return ready_groups.load() != 0;
}

}
