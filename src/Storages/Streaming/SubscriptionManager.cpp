#include <memory>
#include <mutex>

#include <Processors/Chunk.h>

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

void StreamSubscriptionManager::registerSubscription(StreamSubscriptionPtr subscription)
{
    auto lock = lockExclusive();
    subscriptions.push_back(subscription);
    subscriptions_count.fetch_add(1);
}

void StreamSubscriptionManager::executeOnEachSubscription(const std::function<void(StreamSubscriptionPtr & subscription)> & func)
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

        func(locked_sub);
    }

    if (need_clean)
    {
        lock.unlock();
        clean();
    }
}

size_t StreamSubscriptionManager::getSubscriptionsCount() const
{
    return subscriptions_count.load();
}

bool StreamSubscriptionManager::isEmpty() const
{
    return getSubscriptionsCount() == 0;
}

bool StreamSubscriptionManager::hasSome() const
{
    return !isEmpty();
}

void StreamSubscriptionManager::clean()
{
    auto lock = lockExclusive();
    auto it = subscriptions.begin();

    while (it != subscriptions.end())
    {
        if (it->lock() == nullptr)
        {
            subscriptions.erase(it++);
            subscriptions_count.fetch_sub(1);
        }
        else
        {
            ++it;
        }
    }
}

}
