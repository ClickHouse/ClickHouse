#pragma once

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/IProcessor.h>
#include <Common/ListWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

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

    void submitForRemoval(const Processors & processors);
    void markFinished(const VectorWithMemoryTracking<IProcessor *> & processors);

    bool hasReadyForRemoval() const;
    Processors takeReadyForRemoval();
    size_t finishedCount() const;

private:
    mutable Wyrd::Mutex mutex;
    ListWithMemoryTracking<Group> groups;
    UnorderedMapWithMemoryTracking<const IProcessor *, Group *> pending_finish_groups;
    UnorderedSetWithMemoryTracking<const IProcessor *> finished_processors;
    Wyrd::Atomic<size_t> ready_groups{0};
    Wyrd::Atomic<size_t> finished_total{0};
};

}

}
