#include <Storages/MergeTree/PartitionValueIndex.h>

#include <Storages/MergeTree/GenericExclusionSearch.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <algorithm>

namespace DB
{

PartitionValueIndexResult selectPartsByPartitionValueIndex(
    const RangesInDataParts & parts,
    const PartitionCatalog & catalog,
    const KeyCondition & condition,
    size_t coarse_index_granularity,
    size_t max_steps)
{
    chassert(catalog.isFor(parts));

    PartitionValueIndexResult result;

    const auto & partition_set = *catalog.partition_set;
    const size_t num_partitions = partition_set.ids.size();
    const size_t key_size = partition_set.sorted_values.size();
    if (num_partitions == 0)
        return result;

    std::vector<FieldRef> left_keys(key_size);
    std::vector<FieldRef> right_keys(key_size);

    auto load_keys = [&](size_t row, std::vector<FieldRef> & keys)
    {
        for (size_t i = 0; i < key_size; ++i)
        {
            partition_set.sorted_values[i]->get(row, keys[i]);
            /// NULL_LAST
            if (keys[i].isNull())
                keys[i] = POSITIVE_INFINITY;
        }
    };

    /// The partition values at the sorted positions [first, last] lie in the range between the values at
    /// `first` and `last`, so a check of that range is a check of all of them.
    auto check_in_range = [&](size_t first, size_t last)
    {
        load_keys(first, left_keys);
        load_keys(last, right_keys);
        return condition.checkInRange(key_size, left_keys.data(), right_keys.data(), partition_set.types);
    };

    /// Ranges of sorted positions of the selected partitions.
    MarkRanges selected;

    if (condition.matchesExactContinuousRange())
    {
        /// The values for which the condition holds form a continuous range in the order of the partition key, so the
        /// selected partitions are the ones between the first and the last position where it can hold.
        result.search_algorithm = MarkRanges::SearchAlgorithm::BinarySearch;

        auto can_be_true = [&](size_t first, size_t last)
        {
            ++result.num_steps;
            load_keys(first, left_keys);
            load_keys(last, right_keys);
            return condition.mayBeTrueInRange(key_size, left_keys.data(), right_keys.data(), partition_set.types);
        };

        const size_t last_position = num_partitions - 1;
        if (can_be_true(0, last_position))
        {
            /// The first position `p` such that the condition can be true in [0, p].
            size_t low = 0;
            size_t high = last_position;
            while (low < high)
            {
                size_t middle = low + (high - low) / 2;
                if (can_be_true(0, middle))
                    high = middle;
                else
                    low = middle + 1;
            }
            const size_t first = low;

            /// The last position `p` such that the condition can be true in [p, last_position].
            high = last_position;
            while (low < high)
            {
                size_t middle = low + (high - low + 1) / 2;
                if (can_be_true(middle, last_position))
                    low = middle;
                else
                    high = middle - 1;
            }

            selected.emplace_back(first, low + 1);
        }
    }
    else
    {
        result.search_algorithm = MarkRanges::SearchAlgorithm::GenericExclusionSearch;

        /// Each check costs about as much as checking a few parts one by one, so by default bound the number of checks
        /// by a fraction of the number of partitions. If the budget is spent, the remaining partitions are selected whole.
        GenericExclusionSearchSettings search_settings{
            .coarse_index_granularity = coarse_index_granularity,
            .max_steps = max_steps ? max_steps : std::max<size_t>(64, num_partitions / 16),
            .min_marks_for_seek = 0,
        };

        auto search_result = genericExclusionSearch(
            MarkRanges{MarkRange(0, num_partitions)},
            [&](const MarkRange & range) { return check_in_range(range.begin, range.end - 1); },
            search_settings,
            /*collect_exact_ranges=*/false);

        selected = std::move(search_result.ranges);
        result.num_steps = search_result.num_steps;
        result.reached_step_limit = search_result.reached_step_limit;
    }

    /// Positions of the selected partitions in `partition_set.ids`, which is also the order of their parts.
    std::vector<size_t> selected_partitions;
    for (const auto & range : selected)
    {
        for (size_t position = range.begin; position < range.end; ++position)
            selected_partitions.push_back(partition_set.value_order[position]);
    }
    std::sort(selected_partitions.begin(), selected_partitions.end());
    result.num_selected_partitions = selected_partitions.size();

    for (size_t partition : selected_partitions)
    {
        const auto [first_part, last_part] = catalog.part_ranges[partition];
        for (size_t i = first_part; i < last_part; ++i)
        {
            result.parts.push_back(parts[i]);
            result.num_granules += parts[i].data_part->index_granularity->getMarksCountWithoutFinal();
        }
    }

    return result;
}

}
