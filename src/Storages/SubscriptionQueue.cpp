#include <memory>
#include <mutex>
#include <utility>

#include <Processors/Chunk.h>

#include <Storages/SubscriptionQueue.h>

namespace DB
{

#if defined(OS_LINUX)

void Subscriber::push(Chunk chunk)
{
    {
        std::unique_lock guard(mutex);
        ready_chunks.emplace_back(std::move(chunk));
    }
    new_chunks_event.write(1);
}

std::list<Chunk> Subscriber::extractAll()
{
    new_chunks_event.read();
    std::unique_lock guard(mutex);
    return std::exchange(ready_chunks, {});
}

std::optional<int> Subscriber::fd() const
{
    return new_chunks_event.fd;
}

void Subscriber::cancel()
{
    new_chunks_event.write(1);
}

#else

void Subscriber::push(Chunk chunk)
{
    std::unique_lock guard(mutex);
    ready_chunks.emplace_back(std::move(chunk));
    empty_chunks.notify_one();
}

std::list<Chunk> Subscriber::extractAll()
{
    std::unique_lock guard(mutex);

    while (ready_chunks.empty() && !cancelled)
        empty_chunks.wait(guard);

    return std::exchange(ready_chunks, {});
}

std::optional<int> Subscriber::fd() const
{
    return std::nullopt;
}

void Subscriber::cancel()
{
    std::unique_lock guard(mutex);
    cancelled = true;
    empty_chunks.notify_one();
}

#endif

std::shared_lock<std::shared_mutex> SubscriptionQueue::lockShared() const
{
    return std::shared_lock{rwlock};
}

std::unique_lock<std::shared_mutex> SubscriptionQueue::lockExclusive() const
{
    return std::unique_lock{rwlock};
}

SubscriberPtr SubscriptionQueue::subscribe()
{
    auto lock = lockExclusive();

    auto sub = std::make_shared<Subscriber>();
    subscribers.push_back(sub);

    return sub;
}

void SubscriptionQueue::pushChunk(Chunk chunk)
{
    auto lock = lockShared();

    bool need_clean = false;
    for (const auto & sub : subscribers)
    {
        auto locked_sub = sub.lock();

        if (locked_sub == nullptr)
        {
            need_clean = true;
            continue;
        }

        locked_sub->push(chunk.clone());
    }

    if (need_clean)
    {
        lock.unlock();
        clean();
    }
}

void SubscriptionQueue::clean()
{
    auto lock = lockExclusive();
    auto it = subscribers.begin();

    while (it != subscribers.end())
        if (it->lock() == nullptr)
            subscribers.erase(it++);
        else
            ++it;
}

}
