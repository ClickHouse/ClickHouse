#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/IProcessor.h>

#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace Runtime::V2
{

class RemovalGroups
{
    struct Group
    {
        Processors processors;
        size_t unfinished = 0;
    };

public:
    RemovalGroups();

    void submitForRemoval(Processors processors);
    void markFinished(IProcessor * processor);

    bool hasReadyForRemoval() const;
    Processors takeReadyForRemoval();

private:
    Wyrd::Mutex mutex;
    std::list<Group> groups;
    std::unordered_map<const IProcessor *, Group *> pending_finish_groups;
    std::unordered_set<const IProcessor *> finished_processors;
    Wyrd::Atomic<size_t> ready_groups{0};
};

}

}
