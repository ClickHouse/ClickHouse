#pragma once

#include <Processors/Executors/Runtime/Engine/Task.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>

namespace DB
{

class WorkStealingQueue
{
public:
    void pushBack(Task task);
    void pushFront(Task task);
    Task popFront();
    Task popBack();

    size_t takeFirst(WorkStealingQueue & victim, size_t max_to_take);
    size_t takeAll(WorkStealingQueue & victim);

    bool empty() const;
    size_t size() const;

private:
    boost::container::devector<Task, AllocatorWithMemoryTracking<Task>> tasks;
};

}
