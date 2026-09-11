#pragma once

#include <Processors/Executors/Runtime/Engine/Task.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>

namespace DB
{

class TaskQueue
{
public:
    void pushBack(Task task);
    void pushFront(Task task);
    Task popFront();
    Task popBack();

    void takeFront(TaskQueue & from, size_t count);
    void takeAll(TaskQueue & from);

    bool empty() const;
    size_t size() const;

private:
    boost::container::devector<Task, AllocatorWithMemoryTracking<Task>> tasks;
};

}
