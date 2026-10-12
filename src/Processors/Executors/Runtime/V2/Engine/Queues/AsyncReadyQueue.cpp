#include <Processors/Executors/Runtime/V2/Engine/Queues/AsyncReadyQueue.h>

#include <algorithm>
#include <mutex>

namespace DB
{

namespace Runtime::V2
{

AsyncReadyQueue::AsyncReadyQueue()
{
    Wyrd::setName(mutex, "async_ready.mutex");
    Wyrd::setName(count, "async_ready.count");
}

void AsyncReadyQueue::pushMany(std::span<ExecutingProcessor * const> many)
{
    if (many.empty())
        return;

    std::lock_guard lock(mutex);
    entries.insert(entries.end(), many.begin(), many.end());
    count.store(entries.size());
}

size_t AsyncReadyQueue::popManyLifo(std::span<ExecutingProcessor *> out, size_t sharers)
{
    if (count.load() == 0)
        return 0;

    std::lock_guard lock(mutex);
    const size_t taken = std::min({out.size(), entries.size(), entries.size() / sharers + 1});
    std::copy_n(entries.end() - taken, taken, out.begin());
    entries.resize(entries.size() - taken);
    count.store(entries.size());
    return taken;
}

size_t AsyncReadyQueue::size() const
{
    return count.load();
}

}

}
