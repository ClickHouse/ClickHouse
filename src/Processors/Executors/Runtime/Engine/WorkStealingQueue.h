#pragma once

#include <Processors/Executors/Runtime/Engine/Task.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>

namespace DB
{

class WorkStealingQueue
{
public:
    void push(Task task);
    Task pop();

    size_t takeFront(WorkStealingQueue & victim, size_t max_to_take = 7);
    size_t takeBack(WorkStealingQueue & victim, size_t max_to_take = 7);

    bool empty() const;
    size_t size() const;

private:
    boost::container::devector<Task, AllocatorWithMemoryTracking<Task>> tasks;
};

}
