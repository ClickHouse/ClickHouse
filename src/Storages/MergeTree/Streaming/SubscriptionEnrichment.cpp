#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>
#include <Storages/MergeTree/Streaming/SubscriptionEnrichment.h>

namespace DB
{

namespace
{

bool isPartitionConnectsToSubscription(const String & partition_id, size_t subscriptions_count, size_t subscription_index)
{
    size_t partition_hash = std::hash<String>{}(partition_id);
    return partition_hash % subscriptions_count == subscription_index;
}

template <class T>
class SubscriptionEnricher
{
    bool canPromote(const String & partition_id, const Int64 & max_block_number, const MergeTreePartInfo & info) const
    {
        return (info.min_block <= max_block_number) || promoters.at(partition_id).canPromote(max_block_number, info.min_block);
    }

    bool processPart(const String & partition_id, const MergeTreeData::DataPartPtr & part, Int64 & max_block_number)
    {
        chassert(part != nullptr);

        if (canPromote(partition_id, max_block_number, part->info))
        {
            auto alter_convertions = storage.getAlterConversionsForPart(part);
            subscription->push(RangesInDataPart(part, alter_convertions, 0, MarkRanges{MarkRange(0, part->getMarksCount())}));

            max_block_number = part->info.max_block;

            return true;
        }

        return false;
    }

    bool processPart(const String & partition_id, const RangesInDataPart & ranges, Int64 & max_block_number)
    {
        chassert(ranges.data_part != nullptr);
        const auto & part = ranges.data_part;

        if (canPromote(partition_id, max_block_number, part->info))
        {
            subscription->push(ranges);
            max_block_number = part->info.max_block;

            return true;
        }

        return false;
    }

public:
    SubscriptionEnricher(
        StreamSubscriptionPtr subscription_holder_,
        const MergeTreeData & storage_,
        const std::map<String, std::map<Int64, T>> & data_parts_,
        const CursorPromotersMap & promoters_)
        : subscription(subscription_holder_->as<RangesInDataPartStreamSubscription>())
        , storage(storage_)
        , data_parts(data_parts_)
        , promoters(promoters_)
    {
        chassert(subscription != nullptr);
    }

    bool enrichSubscription()
    {
        bool enriched = false;

        for (const auto & [partition_id, parts] : data_parts)
        {
            if (!isPartitionConnectsToSubscription(
                    partition_id, subscription->query_subscriptions_count, subscription->current_subscription_index))
                continue;

            auto [max_block_number_it, _] = subscription->max_block_numbers.try_emplace(partition_id, -1);
            chassert(max_block_number_it != subscription->max_block_numbers.end());
            Int64 & max_block_number = max_block_number_it->second;

            for (auto part_it = parts.upper_bound(max_block_number); part_it != parts.end(); ++part_it)
                if (processPart(partition_id, part_it->second, max_block_number))
                    enriched = true;
                else
                    break;
        }

        return enriched;
    }

private:
    RangesInDataPartStreamSubscription * subscription;
    const MergeTreeData & storage;
    const std::map<String, std::map<Int64, T>> & data_parts;
    const CursorPromotersMap & promoters;

    Poco::Logger * log = &Poco::Logger::get("SubscriptionEnricher");
};

}

bool enrichSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> & data_parts,
    const CursorPromotersMap & promoters)
{
    SubscriptionEnricher enricher(std::move(subscription_holder), storage, data_parts, promoters);
    return enricher.enrichSubscription();
}

bool enrichSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, RangesInDataPart>> & data_parts,
    const CursorPromotersMap & promoters)
{
    SubscriptionEnricher enricher(std::move(subscription_holder), storage, data_parts, promoters);
    return enricher.enrichSubscription();
}

std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> buildRightPartsIndex(MergeTreeData::DataPartsVector data_parts)
{
    std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> index;

    for (auto && part : data_parts)
        index[part->info.partition_id][part->info.max_block] = std::move(part);

    return index;
}

std::map<String, std::map<Int64, RangesInDataPart>> buildRightPartsIndex(RangesInDataParts parts_ranges)
{
    std::map<String, std::map<Int64, RangesInDataPart>> index;

    for (auto && ranges : parts_ranges)
        index[ranges.data_part->info.partition_id][ranges.data_part->info.max_block] = std::move(ranges);

    return index;
}

CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges)
{
    CursorPromotersMap promoters;

    for (auto && [partition_id, ranges] : partition_ranges)
        if (auto it = committing_block_numbers.find(partition_id); it != committing_block_numbers.end())
            promoters.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(partition_id),
                std::forward_as_tuple(std::move(it->second), std::move(ranges)));
        else
            promoters.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(partition_id),
                std::forward_as_tuple(std::set<Int64>{}, std::move(ranges)));

    return promoters;
}

}
