#include <Storages/Streaming/SubscriptionManager.h>

namespace DB
{

std::shared_lock<SharedMutex> StreamSubscriptionManager::lockShared() const
{
    return std::shared_lock{rwlock};
}

std::unique_lock<SharedMutex> StreamSubscriptionManager::lockExclusive() const
{
    return std::unique_lock{rwlock};
}

void StreamSubscriptionManager::registerSubscription(StreamSubscriptionPtr subscription)
{
    auto lock = lockExclusive();
    subscriptions.push_back(subscription);
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

bool StreamSubscriptionManager::isEmpty() const
{
    auto lock = lockShared();

    for (const auto & subscription : subscriptions)
        if (!subscription.expired())
            return false;

    return true;
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
            it = subscriptions.erase(it);
        else
            ++it;
    }
}

}
