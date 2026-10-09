#include <Processors/Executors/Runtime/V2/Pipeline/State/RemovalGroups.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Runtime::V2
{

RemovalGroups::RemovalGroups()
{
    Wyrd::setName(mutex, "removal_groups.mutex");
    Wyrd::setName(ready_groups, "removal_groups.ready_groups");
}

void RemovalGroups::submitForRemoval(Processors processors)
{
    std::lock_guard lock(mutex);

    Group & group = groups.emplace_back();
    group.processors = std::move(processors);

    for (const auto & processor : group.processors)
    {
        if (finished_processors.erase(processor.get()))
            continue;

        if (!pending_finish_groups.emplace(processor.get(), &group).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} is already pending_finish_groups removal", processor->getName());

        ++group.unfinished;
    }

    if (group.unfinished == 0)
        ready_groups.fetch_add(1);
}

void RemovalGroups::markFinished(IProcessor * processor)
{
    std::lock_guard lock(mutex);

    auto it = pending_finish_groups.find(processor);
    if (it == pending_finish_groups.end())
    {
        finished_processors.insert(processor);
        return;
    }

    Group & group = *it->second;
    group.unfinished -= 1;
    pending_finish_groups.erase(it);

    if (group.unfinished == 0)
        ready_groups.fetch_add(1);
}

Processors RemovalGroups::takeReadyForRemoval()
{
    std::lock_guard lock(mutex);

    Processors result;
    for (auto it = groups.begin(); it != groups.end();)
    {
        if (it->unfinished != 0)
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

bool RemovalGroups::hasReadyForRemoval() const
{
    return ready_groups.load() != 0;
}

}

}
