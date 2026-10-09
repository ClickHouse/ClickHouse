#include <Processors/Executors/Runtime/V2/Engine/Tasks/GlobalTasks.h>

#include <algorithm>
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

void GlobalTasks::pushManyFifo(std::span<const Task> many)
{
    if (many.empty())
        return;

    std::lock_guard lock(mutex);
    tasks.insert(tasks.end(), many.begin(), many.end());
    tasks_count.store(tasks.size());
}

size_t GlobalTasks::popManyFifo(std::span<Task> out, size_t sharers)
{
    if (tasks_count.load() == 0)
        return 0;

    std::lock_guard lock(mutex);
    const size_t count = std::min({out.size(), tasks.size(), tasks.size() / sharers + 1});
    std::copy_n(tasks.begin(), count, out.begin());
    tasks.erase(tasks.begin(), tasks.begin() + count);
    tasks_count.store(tasks.size());
    return count;
}

size_t GlobalTasks::size() const
{
    return tasks_count.load();
}

}

}
