#pragma once

#include <Processors/IProcessor_fwd.h>

#include <atomic>
#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

class RemovalCoordinator
{
    struct Group
    {
        Processors processors;
        size_t not_finished = 0;
    };

public:
    void submit(Processors processors);
    void onFinished(IProcessor * processor);
    Processors takeReadyForRemoval();

    bool hasReady() const;

private:
    std::mutex mutex;
    std::list<Group> groups;
    std::unordered_map<const IProcessor *, Group *> pending;
    std::unordered_set<const IProcessor *> finished;
    std::atomic<size_t> ready_groups{0};
};

}
