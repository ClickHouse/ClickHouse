#include <Processors/Executors/Runtime/V2/Engine/Tasks/GlobalTasks.h>

#include <mutex>

namespace DB
{

namespace Runtime::V2
{

void GlobalTasks::push(Task task)
{
    std::lock_guard lock(mutex);
    tasks.pushBack(task);
    tasks_count.store(tasks.size());
}

void GlobalTasks::takeFront(TaskQueue & from, size_t count)
{
    std::lock_guard lock(mutex);
    tasks.takeFront(from, count);
    tasks_count.store(tasks.size());
}

void GlobalTasks::takeAll(TaskQueue & from)
{
    takeFront(from, from.size());
}

std::optional<Task> GlobalTasks::tryPop()
{
    if (tasks_count.load() == 0)
        return std::nullopt;

    std::lock_guard lock(mutex);
    if (tasks.empty())
        return std::nullopt;

    Task task = tasks.popFront();
    tasks_count.store(tasks.size());
    return task;
}

size_t GlobalTasks::size() const
{
    return tasks_count.load();
}

}

}
