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

std::vector<StreamSubscriptionPtr> StreamSubscriptionManager::takeAllSubscriptions()
{
    std::vector<StreamSubscriptionPtr> collected;
    bool need_clean = false;

    {
        auto lock = lockShared();
        collected.reserve(subscriptions.size());

        for (const auto & subscription : subscriptions)
        {
            if (auto locked_sub = subscription.lock())
                collected.push_back(std::move(locked_sub));
            else
                need_clean = true;
        }
    }

    if (need_clean)
        clean();

    return collected;
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
