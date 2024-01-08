#include <mutex>
#include <utility>

#include <Processors/Chunk.h>

#include <Storages/Streaming/Subscription.h>

namespace DB
{

#if defined(OS_LINUX)

void StreamSubscription::push(Chunk chunk)
{
    {
        std::unique_lock guard(mutex);
        ready_chunks.emplace_back(std::move(chunk));
    }
    new_chunks_event.write(1);
}

std::list<Chunk> StreamSubscription::extractAll()
{
    if (is_disabled.load())
        return {};

    new_chunks_event.read();

    std::unique_lock guard(mutex);
    return std::exchange(ready_chunks, {});
}

std::optional<int> StreamSubscription::fd() const
{
    return new_chunks_event.fd;
}

void StreamSubscription::disable()
{
    is_disabled.store(true);
    new_chunks_event.write(1);
}

#else

void StreamSubscription::push(Chunk chunk)
{
    std::unique_lock guard(mutex);
    ready_chunks.emplace_back(std::move(chunk));
    empty_chunks.notify_one();
}

std::list<Chunk> StreamSubscription::extractAll()
{
    std::unique_lock guard(mutex);

    while (ready_chunks.empty() && !is_disabled.load())
        empty_chunks.wait(guard);

    return std::exchange(ready_chunks, {});
}

std::optional<int> StreamSubscription::fd() const
{
    return std::nullopt;
}

void StreamSubscription::disable()
{
    std::unique_lock guard(mutex);
    is_disabled.store(true);
    empty_chunks.notify_one();
}

#endif

}
