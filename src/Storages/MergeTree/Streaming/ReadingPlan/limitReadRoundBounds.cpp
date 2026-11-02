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
    const UInt64 max_rows = reading_context.max_rows_per_round;
    const UInt64 max_bytes = reading_context.max_bytes_per_round;
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

            const Int64 low = std::max(part->info.min_block, from);
            const Int64 high = std::min(part->info.max_block, safe_block_number);
            const double blocks_in_part = static_cast<double>(part->info.max_block - part->info.min_block + 1);
            const double rows_per_block = static_cast<double>(part->rows_count) / blocks_in_part;
            const double bytes_per_block = static_cast<double>(part->getBytesUncompressedOnDisk()) / blocks_in_part;
            const double blocks = static_cast<double>(high - low + 1);

            if (rows_per_block * blocks <= rows_budget && bytes_per_block * blocks <= bytes_budget)
            {
                rows_budget -= rows_per_block * blocks;
                bytes_budget -= bytes_per_block * blocks;
                any_part_taken = true;
                continue;
            }

            /// The part crosses the limit: take as many of its blocks as fit, ending the round inside it.
            /// The limit that is crossed has a non-zero size per block, so the result is finite and below `blocks`.
            const auto fit = [](double budget, double per_block) { return per_block > 0 ? budget / per_block : std::numeric_limits<double>::infinity(); };
            const Int64 taken = static_cast<Int64>(std::min(fit(rows_budget, rows_per_block), fit(bytes_budget, bytes_per_block)));
            /// Read at least one block of data per round.
            bound = low + std::max<Int64>(taken, any_part_taken ? 0 : 1) - 1;
            budget_exhausted = true;
            break;
        }
    }

    return safe_block_numbers;
}

}
