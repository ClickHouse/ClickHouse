#include <memory>
#include <mutex>
#include <utility>

#include <Processors/Chunk.h>

#include <Storages/SubscriptionQueue.hpp>

namespace DB
{

void Subscriber::push(Chunk chunk)
{
    std::unique_lock guard(mutex);
    ready_chunks.emplace_back(std::move(chunk));
    event_fd.write(1);
}

std::list<Chunk> Subscriber::extractAll()
{
    std::unique_lock guard(mutex);
    event_fd.read();
    return std::exchange(ready_chunks, {});
}

int Subscriber::fd() const
{
    return event_fd.fd;
}

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
