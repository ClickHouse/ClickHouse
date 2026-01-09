#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>

#include <Common/LoggingFormatStringHelpers.h>
#include <Common/logger_useful.h>

#include <expected>

namespace DB
{

template <class Part, class Predicate>
std::vector<std::vector<Part>> splitRangeByPredicate(std::vector<Part> && parts, Predicate predicate, LogSeriesLimiter & series_log)
{
    auto build_next_range = [&](auto & parts_it)
    {
        std::vector<Part> range;

        while (parts_it != parts.end())
        {
            Part part = std::move(*parts_it++);

            /// Close current range if next part can't be used.
            if (auto result = predicate(part); !result)
            {
                LOG_TRACE(series_log, "Filtered part in collector: {}", result.error().text);
                return range;
            }

            /// Otherwise include part to current range
            range.push_back(std::move(part));
        }

        return range;
    };

    std::vector<MergeTreeDataPartsVector> ranges;
    for (auto part_it = parts.begin(); part_it != parts.end();)
        if (auto next_range = build_next_range(part_it); !next_range.empty())
            ranges.push_back(std::move(next_range));

    return ranges;
}

template <class Part, class Predicate>
std::expected<void, PreformattedMessage> checkAllPartsSatisfyPredicate(const std::vector<Part> & parts, Predicate predicate)
{
    for (const auto & part : parts)
        if (auto result = predicate(part); !result)
            return std::unexpected(result.error());

    return {};
}

PartsRanges constructPartsRanges(
    std::vector<MergeTreeDataPartsVector> && ranges, const StorageMetadataPtr & metadata_snapshot, const time_t & current_time);

MergeTreeDataPartsVector filterByPartitions(
    MergeTreeDataPartsVector && parts, const std::optional<PartitionIdsHint> & partitions_to_keep);

}
