#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>

#include <optional>
#include <span>

namespace DB
{

namespace Runtime::V2
{

class GlobalTasks
{
public:
    GlobalTasks();

    void push(const Task & task);
    void pushMany(std::span<const Task> many);
    std::optional<Task> tryPop();
    size_t size() const;

private:
    Wyrd::Mutex mutex;
    boost::container::devector<Task, AllocatorWithMemoryTracking<Task>> tasks;
    Wyrd::Atomic<size_t> tasks_count = 0;
};

}

}
