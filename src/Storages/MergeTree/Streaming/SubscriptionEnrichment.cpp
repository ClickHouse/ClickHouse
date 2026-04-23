#include <Storages/MergeTree/Streaming/SubscriptionEnrichment.h>

#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>

namespace DB
{

namespace
{

bool partitionBelongsToSubscription(const String & partition_id, size_t subscriptions_count, size_t subscription_index)
{
    return std::hash<String>{}(partition_id) % subscriptions_count == subscription_index;
}

template <class T>
class SubscriptionEnricher
{
    bool canPromote(const String & partition_id, Int64 max_block_number, const MergeTreePartInfo & info) const
    {
        /// This part was produced after merge of some already consumed parts - that means that
        /// some prefix of this part was already consumed by stream, but suffix was not, so here we can
        /// enrich subscription with new part and in runtime filter prefix.
        if (info.min_block <= max_block_number)
            return true;

        auto it = promoters.find(partition_id);
        if (it == promoters.end())
            return true;

        return it->second.canPromote(max_block_number, info.min_block);
    }

    bool tryPush(const String & partition_id, const MergeTreeData::DataPartPtr & part, Int64 & max_block_number)
    {
        if (!canPromote(partition_id, max_block_number, part->info))
            return false;

        subscription->push(RangesInDataPart(
            part,
            /*parent_part=*/ nullptr,
            /*part_index_in_query=*/ 0,
            /*part_starting_offset_in_query=*/ 0,
            MarkRanges{MarkRange(0, part->getMarksCount())}, /// TODO: run index analysis here
            /*read_hints=*/ {}));
        max_block_number = part->info.max_block;

        return true;
    }

    bool tryPush(const String & partition_id, const RangesInDataPart & ranges, Int64 & max_block_number)
    {
        if (!canPromote(partition_id, max_block_number, ranges.data_part->info))
            return false;

        subscription->push(ranges);
        max_block_number = ranges.data_part->info.max_block;

        return true;
    }

public:
    SubscriptionEnricher(
        StreamSubscriptionPtr subscription_,
        const MergeTreeData & storage_,
        const std::map<String, std::map<Int64, T>> & data_parts_,
        const CursorPromotersMap & promoters_)
        : subscription(subscription_->as<RangesInDataPartStreamSubscription>())
        , storage(storage_)
        , data_parts(data_parts_)
        , promoters(promoters_)
    {
        chassert(subscription != nullptr);
    }

    bool run()
    {
        bool enriched = false;

        for (const auto & [partition_id, parts] : data_parts)
        {
            if (!partitionBelongsToSubscription(partition_id, subscription->query_subscriptions_count, subscription->current_subscription_index))
                continue;

            auto [it, _] = subscription->max_block_numbers.try_emplace(partition_id, -1);
            Int64 & max_block_number = it->second;

            /// Parts are keyed by max_block. `upper_bound(max_block_number)` skips parts
            /// whose entire range is at-or-below the already pushed max_block_number.
            for (auto part_it = parts.upper_bound(max_block_number); part_it != parts.end(); ++part_it)
            {
                if (!tryPush(partition_id, part_it->second, max_block_number))
                    break;

                enriched = true;
            }
        }

        return enriched;
    }

private:
    RangesInDataPartStreamSubscription * subscription;
    const MergeTreeData & storage;
    const std::map<String, std::map<Int64, T>> & data_parts;
    const CursorPromotersMap & promoters;
};

}

bool enrichSubscription(
    StreamSubscriptionPtr subscription,
    const MergeTreeData & storage,
    const PartsByPartitionAndBlockNumber & data_parts,
    const CursorPromotersMap & promoters)
{
    return SubscriptionEnricher<MergeTreeData::DataPartPtr>(std::move(subscription), storage, data_parts, promoters).run();
}

bool enrichSubscription(
    StreamSubscriptionPtr subscription,
    const MergeTreeData & storage,
    const RangesByPartitionAndBlockNumber & data_parts,
    const CursorPromotersMap & promoters)
{
    return SubscriptionEnricher<RangesInDataPart>(std::move(subscription), storage, data_parts, promoters).run();
}

PartsByPartitionAndBlockNumber buildRightPartsIndex(MergeTreeData::DataPartsVector data_parts)
{
    PartsByPartitionAndBlockNumber index;
    for (auto && part : data_parts)
        index[part->info.getPartitionId()][part->info.max_block] = std::move(part);
    return index;
}

RangesByPartitionAndBlockNumber buildRightPartsIndex(RangesInDataParts parts_ranges)
{
    RangesByPartitionAndBlockNumber index;
    for (auto && ranges : parts_ranges)
        index[ranges.data_part->info.getPartitionId()].emplace(ranges.data_part->info.max_block, std::move(ranges));
    return index;
}

CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges)
{
    CursorPromotersMap promoters;

    for (auto && [partition_id, ranges] : partition_ranges)
    {
        auto committing_it = committing_block_numbers.find(partition_id);
        auto committing = (committing_it != committing_block_numbers.end())
            ? std::move(committing_it->second)
            : std::set<Int64>{};

        promoters.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(partition_id),
            std::forward_as_tuple(std::move(committing), std::move(ranges)));
    }

    return promoters;
}

}
