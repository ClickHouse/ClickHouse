#include <Processors/Executors/Runtime/V2/Pipeline/RemovalGroups.h>
#include <Processors/IProcessor.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>

namespace DB
{

namespace Runtime::V2
{

RemovalGroups::RemovalGroups()
{
    Wyrd::setName(mutex, "removal_groups.mutex");
    Wyrd::setName(ready_groups, "removal_groups.ready_groups");
    Wyrd::setName(finished_total, "removal_groups.finished_total");
}

void RemovalGroups::submitForRemoval(const Processors & processors)
{
    std::lock_guard lock(mutex);

    Group & group = groups.emplace_back();
    group.processors = processors;

    for (const auto & processor : group.processors)
    {
        if (finished_processors.erase(processor.get()))
            continue;

        if (!pending_finish_groups.emplace(processor.get(), &group).second)
            Wyrd::abort("Processor {} is already submitted for removal", processor->getName());

        ++group.unfinished;
    }

    if (group.unfinished == 0)
        ready_groups.fetch_add(1);
}

void RemovalGroups::markFinished(const VectorWithMemoryTracking<IProcessor *> & processors)
{
    if (processors.empty())
        return;

    std::lock_guard lock(mutex);

    size_t groups_completed = 0;
    for (auto * processor : processors)
    {
        auto it = pending_finish_groups.find(processor);
        if (it == pending_finish_groups.end())
        {
            finished_processors.insert(processor);
            continue;
        }

        Group & group = *it->second;
        group.unfinished -= 1;
        pending_finish_groups.erase(it);

        if (group.unfinished == 0)
            ++groups_completed;
    }

    finished_total.fetch_add(processors.size());
    if (groups_completed > 0)
        ready_groups.fetch_add(groups_completed);
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
    finished_total.fetch_sub(result.size());
    return result;
}

size_t RemovalGroups::finishedCount() const
{
    return finished_total.load();
}

bool RemovalGroups::hasReadyForRemoval() const
{
    return ready_groups.load() != 0;
}

}

}
