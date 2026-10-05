#pragma once

#include <Processors/IProcessor_fwd.h>

#include <atomic>
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
    void submitForRemoval(Processors processors);
    void markFinished(IProcessor * processor);

    bool hasReadyForRemoval() const;
    Processors takeReadyForRemoval();

private:
    std::mutex mutex;
    std::list<Group> groups;
    std::unordered_map<const IProcessor *, Group *> pending_finish_groups;
    std::unordered_set<const IProcessor *> finished_processors;
    std::atomic<size_t> ready_groups{0};
};

}

}
