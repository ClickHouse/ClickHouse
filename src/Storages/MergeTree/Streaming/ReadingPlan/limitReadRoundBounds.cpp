#include <Storages/MergeTree/Streaming/ReadingPlan/limitReadRoundBounds.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <algorithm>
#include <limits>
#include <tuple>
#include <vector>

namespace DB
{

std::map<String, Int64> limitReadRoundBounds(
    const ReadRoundContext & reading_context,
    const ReadState & state,
    std::map<String, Int64> safe_block_numbers)
{
    const auto [max_rows, max_bytes] = getReadRoundLimits(reading_context.storage_limits.get());
    if (!max_rows && !max_bytes)
        return safe_block_numbers;

    struct ChangedPartition
    {
        String partition_id;
        MergeTreeData::DataPartsVector parts;
        /// Modification time of the first part that is not read yet.
        time_t next_part_time = std::numeric_limits<time_t>::max();
    };

    std::vector<ChangedPartition> changed_partitions;
    for (const auto & [partition_id, safe_block_number] : safe_block_numbers)
    {
        const Int64 from = state.getPartitionCursor(partition_id).block_number;
        if (from > safe_block_number)
            continue;

        auto parts = reading_context.storage.getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition_id);
        std::ranges::sort(parts, {}, [](const auto & part) { return part->info.min_block; });

        ChangedPartition partition{.partition_id = partition_id, .parts = std::move(parts)};
        for (const auto & part : partition.parts)
        {
            if (part->info.max_block >= from && part->info.min_block <= safe_block_number)
            {
                partition.next_part_time = part->modification_time;
                break;
            }
        }
        changed_partitions.push_back(std::move(partition));
    }

    /// The partition that waits longest goes first, so a busy partition cannot hold the others back.
    std::ranges::sort(changed_partitions, {}, [](const auto & partition) { return std::tie(partition.next_part_time, partition.partition_id); });

    /// Whole parts only: a merged part may have gaps in its block numbers, so a bound inside it can select no rows.
    double rows_budget = max_rows ? static_cast<double>(max_rows) : std::numeric_limits<double>::infinity();
    double bytes_budget = max_bytes ? static_cast<double>(max_bytes) : std::numeric_limits<double>::infinity();
    bool budget_exhausted = false;
    bool any_part_taken = false;

    for (const auto & partition : changed_partitions)
    {
        const Int64 from = state.getPartitionCursor(partition.partition_id).block_number;
        Int64 & bound = safe_block_numbers.at(partition.partition_id);

        if (budget_exhausted)
        {
            bound = from - 1;
            continue;
        }

        const Int64 safe_block_number = bound;
        for (const auto & part : partition.parts)
        {
            if (part->info.max_block < from || part->info.min_block > safe_block_number)
                continue;

            const auto rows = static_cast<double>(part->rows_count);
            const auto bytes = static_cast<double>(part->getBytesUncompressedOnDisk());

            /// Read at least one part per round.
            if (any_part_taken && (rows > rows_budget || bytes > bytes_budget))
            {
                bound = std::max(part->info.min_block, from) - 1;
                budget_exhausted = true;
                break;
            }

            rows_budget -= rows;
            bytes_budget -= bytes;
            any_part_taken = true;
        }
    }

    return safe_block_numbers;
}

}
