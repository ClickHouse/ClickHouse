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

    wake.notify();
}

void MergeTreeBoundsSubscription::onEnrichmentRound()
{
    {
        std::lock_guard guard(mutex);
        if (is_disabled)
            return;
        ++updates_count;
    }

    /// Wake readers once the round has finished advancing the map, avoiding a partial mid-round read.
    wake.notify();
}

size_t MergeTreeBoundsSubscription::updatesCount() const
{
    std::lock_guard guard(mutex);
    return updates_count;
}

}
