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
    new_chunks_event.read();
    std::unique_lock guard(mutex);
    return std::exchange(ready_chunks, {});
}

std::optional<int> StreamSubscription::fd() const
{
    return new_chunks_event.fd;
}

void StreamSubscription::cancel()
{
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

    while (ready_chunks.empty() && !cancelled)
        empty_chunks.wait(guard);

    return std::exchange(ready_chunks, {});
}

std::optional<int> StreamSubscription::fd() const
{
    return std::nullopt;
}

void StreamSubscription::cancel()
{
    std::unique_lock guard(mutex);
    cancelled = true;
    empty_chunks.notify_one();
}

#endif

}
