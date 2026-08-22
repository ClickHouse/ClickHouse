#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>

#include <utility>

namespace DB
{

MergeTreeBoundsSubscription::MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_)
    : query_subscriptions_count(query_subscriptions_count_)
    , current_subscription_index(current_subscription_index_)
{
}

bool MergeTreeBoundsSubscription::update(std::map<std::string, int64_t> promited_partitions, std::set<std::string> removed_partitions)
{
    bool changed = false;

    {
        std::lock_guard guard(mutex);
        if (is_disabled)
            return false;

        for (const auto & [partition_id, new_cursor] : promited_partitions)
            safe_block_numbers[partition_id] = new_cursor;

        for (const auto & partition_id : removed_partitions)
            safe_block_numbers.erase(partition_id);

        /// Notify when something changed or on first update.
        const bool is_first_update = !std::exchange(was_updated, true);
        changed = is_first_update || !promited_partitions.empty() || !removed_partitions.empty();
    }

    if (changed)
        wake.notify();

    return changed;
}

MergeTreeBoundsSubscription::Snapshot MergeTreeBoundsSubscription::snapshot() const
{
    std::lock_guard guard(mutex);
    return {safe_block_numbers, was_updated};
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

}
