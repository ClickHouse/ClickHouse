#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>

namespace DB
{

MergeTreeBoundsSubscription::MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_)
    : query_subscriptions_count(query_subscriptions_count_)
    , current_subscription_index(current_subscription_index_)
{
}

void MergeTreeBoundsSubscription::advance(const String & partition_id, Int64 new_cursor)
{
    {
        std::lock_guard guard(mutex);
        if (is_disabled)
            return;

        auto [it, inserted] = safe_block_numbers.try_emplace(partition_id, new_cursor);
        if (!inserted)
        {
            chassert(new_cursor > it->second);
            it->second = new_cursor;
        }
    }

#if defined(OS_LINUX)
    wake.write(1);
#else
    std::lock_guard guard(mutex);
    wake.notify_one();
#endif
}

std::map<String, Int64> MergeTreeBoundsSubscription::snapshot() const
{
    std::lock_guard guard(mutex);
    return safe_block_numbers;
}

bool MergeTreeBoundsSubscription::isDisabled() const
{
    std::lock_guard guard(mutex);
    return is_disabled;
}

void MergeTreeBoundsSubscription::disable()
{
    {
        std::lock_guard guard(mutex);
        is_disabled = true;
    }

#if defined(OS_LINUX)
    wake.write(1);
#else
    wake.notify_all();
#endif
}

EventFD * MergeTreeBoundsSubscription::fd()
{
#if defined(OS_LINUX)
    return &wake;
#else
    return nullptr;
#endif
}

void MergeTreeBoundsSubscription::wait()
{
#if defined(OS_LINUX)
    chassert(false);
#else
    std::unique_lock guard(mutex);
    wake.wait(guard, [this] { return is_disabled; });
#endif
}

}
