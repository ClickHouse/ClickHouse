#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>

namespace DB
{

namespace Runtime::V2
{

class LocalTasks
{
public:
    void pushBack(Task task);
    void pushFront(Task task);
    Task popFront();
    Task popBack();

    bool empty() const;
    size_t size() const;

private:
    boost::container::devector<Task, AllocatorWithMemoryTracking<Task>> tasks;
};

}

}
