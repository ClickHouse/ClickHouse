#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/TaskQueue.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <optional>

namespace DB
{

namespace Runtime::V2
{

class GlobalTasks
{
public:
    void push(Task task);
    void takeFront(TaskQueue & from, size_t count);
    void takeAll(TaskQueue & from);
    std::optional<Task> tryPop();
    size_t size() const;

private:
    Wyrd::Mutex mutex;
    TaskQueue tasks;
    Wyrd::Atomic<size_t> tasks_count = 0;
};

}

}
