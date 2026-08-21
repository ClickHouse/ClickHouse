#include <Storages/MergeTree/Streaming/SubscriptionEnrichment.h>

namespace DB
{

static bool partitionBelongsToSubscription(const String & partition_id, size_t subscriptions_count, size_t subscription_index)
{
    return std::hash<String>{}(partition_id) % subscriptions_count == subscription_index;
}

bool enrichSubscription(
    MergeTreeBoundsSubscription & subscription,
    const LocalPartsByPartition & local_parts,
    const CursorPromotersMap & promoters)
{
    auto snapshot = subscription.snapshot();
    bool enriched = false;

    for (const auto & [partition_id, parts] : local_parts)
    {
        /// If partition is not managed by this subscription - skip.
        if (!partitionBelongsToSubscription(partition_id, subscription.query_subscriptions_count, subscription.current_subscription_index))
            continue;

        /// Promoter is unknown - it is not possible to enrich this partition, not enough information.
        if (!promoters.contains(partition_id))
            continue;

        const MergeTreeCursorPromoter & promoter = promoters.at(partition_id);

        Int64 starting_cursor = -1;
        if (auto it = snapshot.find(partition_id); it != snapshot.end())
            starting_cursor = it->second;

        Int64 cursor = starting_cursor;
        for (const auto & part : parts)
        {
            /// Already entirely consumed.
            if (part.max_block <= cursor)
                continue;

            /// Intersects with the cursor or is contiguous to it — we can read through the part without crossing any gap.
            if (part.min_block <= cursor + 1)
            {
                cursor = part.max_block;
                continue;
            }

            /// Ask the promoter whether anything in that gap (cursor, part.min_block) is still in flight.
            if (!promoter.canPromote(cursor, part.min_block))
                break;

            cursor = part.max_block;
        }

        if (cursor > starting_cursor)
        {
            subscription.advance(partition_id, cursor);
            enriched = true;
        }
    }

    return enriched;
}

}
