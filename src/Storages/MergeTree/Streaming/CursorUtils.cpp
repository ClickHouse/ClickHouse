#include <tuple>
#include <vector>

#include <Poco/Logger.h>

#include <boost/algorithm/string/join.hpp>

#include <Common/logger_useful.h>
#include "Core/Streaming/CursorTree.h"

#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>

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
class SubscriptionPopulator
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
            LOG_DEBUG(log, "Populating {} | {} -> {}", partition_id, max_block_number, part->info.max_block);

            auto alter_convertions = storage.getAlterConversionsForPart(part);
            subscription->push(RangesInDataPart(part, alter_convertions, 0, MarkRanges{MarkRange(0, part->getMarksCount())}));

            max_block_number = part->info.max_block;

            return true;
        }

        LOG_DEBUG(log, "Population {} | {} -> {} banned", partition_id, max_block_number, part->info.max_block);

        return false;
    }

    bool processPart(const String & partition_id, const RangesInDataPart & ranges, Int64 & max_block_number)
    {
        chassert(ranges.data_part != nullptr);
        const auto & part = ranges.data_part;

        if (canPromote(partition_id, max_block_number, part->info))
        {
            LOG_DEBUG(log, "Populating {} | {} -> {}", partition_id, max_block_number, part->info.max_block);

            subscription->push(ranges);
            max_block_number = part->info.max_block;

            return true;
        }

        LOG_DEBUG(log, "Population {} | {} -> {} banned", partition_id, max_block_number, part->info.max_block);

        return false;
    }

    void logAtStart() const
    {
        std::vector<String> promoters_dump;
        promoters_dump.reserve(promoters.size());

        for (const auto & [partition_id, promoter] : promoters)
            promoters_dump.push_back(fmt::format("{}: {{{}}}", partition_id, promoter.dumpStructure()));

        LOG_DEBUG(
            log,
            "Started Population of subscription: {}, promoters: {{{}}}",
            subscription->dumpStructure(),
            boost::join(promoters_dump, ", "));
    }

public:
    SubscriptionPopulator(
        StreamSubscriptionPtr subscription_holder_,
        const MergeTreeData & storage_,
        const std::map<String, std::map<Int64, T>> & data_parts_,
        const std::map<String, MergeTreeCursorPromoter> & promoters_)
        : subscription(subscription_holder_->as<RangesInDataPartStreamSubscription>())
        , storage(storage_)
        , data_parts(data_parts_)
        , promoters(promoters_)
    {
        chassert(subscription != nullptr);
    }

    bool populateSubscription()
    {
        logAtStart();

        bool populated = false;

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
                    populated = true;
                else
                    break;
        }

        return populated;
    }

private:
    RangesInDataPartStreamSubscription * subscription;
    const MergeTreeData & storage;
    const std::map<String, std::map<Int64, T>> & data_parts;
    const std::map<String, MergeTreeCursorPromoter> & promoters;

    Poco::Logger * log = &Poco::Logger::get("SubscriptionPopulator");
};

}

bool PartitionCursor::operator<(const PartitionCursor & other) const
{
    return std::tie(block_number, block_offset) < std::tie(other.block_number, other.block_offset);
}

bool PartitionCursor::operator<=(const PartitionCursor & other) const
{
    return std::tie(block_number, block_offset) <= std::tie(other.block_number, other.block_offset);
}

MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree)
{
    MergeTreeCursor cursor;

    for (const auto & [partition_id, node] : *cursor_tree)
    {
        const auto & partition_node = std::get<CursorTreeNodePtr>(node);
        cursor[partition_id] = PartitionCursor{
            .block_number = partition_node->getValue("block_number"),
            .block_offset = partition_node->getValue("block_offset"),
        };
    }

    return cursor;
}

bool populateSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> & data_parts,
    const std::map<String, MergeTreeCursorPromoter> & promoters)
{
    SubscriptionPopulator populator(std::move(subscription_holder), storage, data_parts, promoters);
    return populator.populateSubscription();
}

bool populateSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, RangesInDataPart>> & data_parts,
    const std::map<String, MergeTreeCursorPromoter> & promoters)
{
    SubscriptionPopulator populator(std::move(subscription_holder), storage, data_parts, promoters);
    return populator.populateSubscription();
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


std::map<String, Int64> buildInitialBlockNumberOffsets(
    const MergeTreeCursor & cursor,
    const MergeTreeData::DataPartsVector & snapshot_data_parts,
    const RangesInDataParts & analyzed_data_parts)
{
    std::map<String, Int64> block_number_offsets;

    auto is_covered = [](const String & partition_id, Int64 block_number, const MergeTreeData::DataPartPtr & part)
    {
        if (part->info.partition_id != partition_id)
            return false;

        if (part->info.min_block <= block_number && block_number <= part->info.max_block)
            return true;

        return false;
    };

    for (const auto & [partition_id, data] : cursor)
    {
        bool covered_by_snapshot = false;
        bool covered_by_analysis = false;

        for (const auto & part : snapshot_data_parts)
        {
            if (is_covered(partition_id, data.block_number, part))
            {
                covered_by_snapshot = true;
                break;
            }
        }

        for (const auto & part : analyzed_data_parts)
        {
            if (is_covered(partition_id, data.block_number, part.data_part))
            {
                covered_by_analysis = true;
                break;
            }
        }

        if (covered_by_snapshot)
        {
            if (covered_by_analysis)
                block_number_offsets[partition_id] = data.block_number - 1;
            else
                block_number_offsets[partition_id] = data.block_number;
        }
        else
        {
            chassert(!covered_by_analysis);
            block_number_offsets[partition_id] = data.block_number;
        }
    }

    return block_number_offsets;
}

}
