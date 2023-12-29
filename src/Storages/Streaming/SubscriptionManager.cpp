#include <memory>
#include <mutex>

#include <Processors/Chunk.h>

#include <Storages/Streaming/Subscription.h>
#include <Storages/Streaming/SubscriptionManager.h>

namespace DB
{

std::shared_lock<std::shared_mutex> StreamSubscriptionManager::lockShared() const
{
    return std::shared_lock{rwlock};
}

std::unique_lock<std::shared_mutex> StreamSubscriptionManager::lockExclusive() const
{
    return std::unique_lock{rwlock};
}

StreamSubscriptionPtr StreamSubscriptionManager::subscribe()
{
    auto lock = lockExclusive();

    auto subscription = std::make_shared<StreamSubscription>();
    subscriptions.push_back(subscription);

    return subscription;
}

void StreamSubscriptionManager::pushChunk(Chunk chunk)
{
    auto lock = lockShared();

    bool need_clean = false;
    for (const auto & subscription : subscriptions)
    {
        auto locked_sub = subscription.lock();

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

void StreamSubscriptionManager::clean()
{
    auto lock = lockExclusive();
    auto it = subscriptions.begin();

    while (it != subscriptions.end())
        if (it->lock() == nullptr)
            subscriptions.erase(it++);
        else
            ++it;
}

}
