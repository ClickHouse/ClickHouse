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

    uint64_t cur_snapshot = subscription_id_counter.fetch_add(1);
    auto subscription = std::make_shared<StreamSubscription>(cur_snapshot);

    subscriptions.push_back(subscription);
    subscriptions_count.fetch_add(1);

    return subscription;
}

void StreamSubscriptionManager::pushToAll(Block block, uint64_t snapshot)
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

        // push to subscription only if it is visible in snapshot
        if (locked_sub->getManagerSnapshot() < snapshot)
            locked_sub->push(block);
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

uint64_t StreamSubscriptionManager::getSnapshot() const
{
    return subscription_id_counter.load();
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
