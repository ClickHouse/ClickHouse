#include <Processors/Executors/Runtime/V2/Engine/Tasks/GlobalTasks.h>

#include <mutex>

namespace DB
{

namespace Runtime::V2
{

GlobalTasks::GlobalTasks()
{
    Wyrd::setName(mutex, "global_tasks.mutex");
    Wyrd::setName(tasks_count, "global_tasks.tasks_count");
}

void GlobalTasks::push(const Task & task)
{
    std::lock_guard lock(mutex);
    tasks.push_back(task);
    tasks_count.store(tasks.size());
}

void GlobalTasks::pushMany(std::span<const Task> many)
{
    if (many.empty())
        return;

    std::lock_guard lock(mutex);
    tasks.insert(tasks.end(), many.begin(), many.end());
    tasks_count.store(tasks.size());
}

std::optional<Task> GlobalTasks::tryPop()
{
    if (tasks_count.load() == 0)
        return std::nullopt;

    std::lock_guard lock(mutex);
    if (tasks.empty())
        return std::nullopt;

    Task task = tasks.front();
    tasks.pop_front();
    tasks_count.store(tasks.size());
    return task;
}

size_t GlobalTasks::size() const
{
    return tasks_count.load();
}

}

}
