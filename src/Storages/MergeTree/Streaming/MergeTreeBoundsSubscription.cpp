#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>

namespace DB
{

MergeTreeBoundsSubscription::MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_, bool bounded_)
    : query_subscriptions_count(query_subscriptions_count_)
    , current_subscription_index(current_subscription_index_)
    , bounded(bounded_)
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

    wake.notify();
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

void MergeTreeBoundsSubscription::onEnrichmentRound(bool pending)
{
    {
        std::lock_guard guard(mutex);
        if (is_disabled)
            return;
        safe_segment_determined = !pending;
    }

    /// Wake a bounded source only on a resolved round, so it can finish an empty snapshot. While a
    /// partition is still blocked there is nothing to do; `advance` wakes it when the gap closes.
    if (bounded && !pending)
        wake.notify();
}

bool MergeTreeBoundsSubscription::safeSegmentDetermined() const
{
    std::lock_guard guard(mutex);
    return safe_segment_determined;
}

}
