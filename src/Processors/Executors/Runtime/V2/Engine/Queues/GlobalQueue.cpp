#include <Processors/Executors/Runtime/V2/Engine/Queues/GlobalQueue.h>

#include <algorithm>
#include <mutex>

namespace DB
{

namespace Runtime::V2
{

GlobalQueue::GlobalQueue(size_t capacity)
{
    entries.reserve(capacity);
    Wyrd::setName(mutex, "global_tasks.mutex");
    Wyrd::setName(count, "global_tasks.count");
}

void GlobalQueue::pushManyFifo(std::span<ExecutingProcessor * const> many)
{
    if (many.empty())
        return;

    std::lock_guard lock(mutex);
    entries.insert(entries.end(), many.begin(), many.end());
    count.store(entries.size());
}

void GlobalQueue::pushManyFront(std::span<ExecutingProcessor * const> many)
{
    if (many.empty())
        return;

    std::lock_guard lock(mutex);
    entries.insert(entries.begin(), many.begin(), many.end());
    count.store(entries.size());
}

size_t GlobalQueue::popManyFifo(std::span<ExecutingProcessor *> out, size_t sharers)
{
    if (count.load() == 0)
        return 0;

    std::lock_guard lock(mutex);
    const size_t taken = std::min({out.size(), entries.size(), entries.size() / sharers + 1});
    std::copy_n(entries.begin(), taken, out.begin());
    entries.erase(entries.begin(), entries.begin() + taken);
    count.store(entries.size());
    return taken;
}

size_t GlobalQueue::size() const
{
    return count.load();
}

}

}
