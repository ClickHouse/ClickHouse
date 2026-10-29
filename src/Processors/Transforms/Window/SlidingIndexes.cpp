#include <Processors/Transforms/Window/SlidingIndexes.h>

#include <Core/SortCursor.h>

#include <algorithm>
#include <numeric>
#include <optional>

namespace DB
{

namespace
{

bool haveSameKeys(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row, const std::vector<size_t> & key_indices)
{
    for (const size_t key : key_indices)
        if (lhs[key]->compareAt(lhs_row, rhs_row, *rhs[key], /*nan_direction_hint=*/1) != 0)
            return false;

    return true;
}

std::vector<bool> markPartitionStarts(const Columns & columns, size_t rows_count, const std::vector<size_t> & partition_by_indices, const std::optional<Columns> & previous_partition_key)
{
    std::vector<bool> starts(rows_count, false);
    starts[0] = !previous_partition_key || !haveSameKeys(*previous_partition_key, 0, columns, 0, partition_by_indices);

    size_t next_start = getEqualRangeEndAssumeSorted(columns, partition_by_indices, 0, rows_count, /*nan_direction_hint=*/1);
    while (next_start < rows_count)
    {
        starts[next_start] = true;
        next_start = getEqualRangeEndAssumeSorted(columns, partition_by_indices, next_start, rows_count, /*nan_direction_hint=*/1);
    }

    return starts;
}

bool haveSameCollatedOrderByKeys(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row, const WindowTransformParams & params)
{
    for (size_t i = 0; i < params.order_by_indices.size(); ++i)
    {
        const IColumn & lhs_column = *lhs[params.order_by_indices[i]];
        const IColumn & rhs_column = *rhs[params.order_by_indices[i]];
        const auto & collator = params.window_description.order_by[i].collator;

        const int res = collator && lhs_column.isCollationSupported()
            ? lhs_column.compareAtWithCollation(lhs_row, rhs_row, rhs_column, /*nan_direction_hint=*/1, *collator)
            : lhs_column.compareAt(lhs_row, rhs_row, rhs_column, /*nan_direction_hint=*/1);
        if (res != 0)
            return false;
    }

    return true;
}

/// Values equal under a collation may differ in bytes and interleave in the sorted block, so adjacent
/// rows are compared one by one instead of scanning for a run of equal raw values.
size_t findCollatedEqualRangeEnd(const Columns & columns, size_t begin, size_t end, const WindowTransformParams & params)
{
    size_t row = begin + 1;
    while (row < end && haveSameCollatedOrderByKeys(columns, row - 1, columns, row, params))
        ++row;

    return row;
}

std::vector<int64_t> assignPeerGroupIndexes(
    const Columns & columns,
    size_t rows_count,
    const WindowTransformParams & params,
    bool every_row_is_a_group,
    bool compare_with_collation,
    const std::vector<bool> & partition_starts,
    const std::optional<Columns> & previous_order_key,
    int64_t last_peer_group_index)
{
    std::vector<int64_t> peer_group_indexes(rows_count);
    const std::vector<size_t> & order_by_indices = params.order_by_indices;
    if (!every_row_is_a_group && order_by_indices.empty())
        return peer_group_indexes;

    auto have_same_keys = [&](const Columns & lhs, size_t lhs_row, size_t rhs_row)
    {
        return compare_with_collation
            ? haveSameCollatedOrderByKeys(lhs, lhs_row, columns, rhs_row, params)
            : haveSameKeys(lhs, lhs_row, columns, rhs_row, order_by_indices);
    };

    const bool first_row_continues_last_group = !every_row_is_a_group && previous_order_key && have_same_keys(*previous_order_key, 0, 0);
    int64_t group = partition_starts[0] ? 0 : (first_row_continues_last_group ? last_peer_group_index : last_peer_group_index + 1);

    size_t partition_begin = 0;
    while (partition_begin < rows_count)
    {
        const size_t partition_end = std::find(partition_starts.begin() + partition_begin + 1, partition_starts.end(), true) - partition_starts.begin();
        if (every_row_is_a_group)
        {
            std::iota(peer_group_indexes.begin() + partition_begin, peer_group_indexes.begin() + partition_end, group);
        }
        else
        {
            size_t run_begin = partition_begin;
            while (run_begin < partition_end)
            {
                const size_t run_end = compare_with_collation
                    ? findCollatedEqualRangeEnd(columns, run_begin, partition_end, params)
                    : getEqualRangeEndAssumeSorted(columns, order_by_indices, run_begin, partition_end, /*nan_direction_hint=*/1);
                std::fill(peer_group_indexes.begin() + run_begin, peer_group_indexes.begin() + run_end, group);
                group += 1;
                run_begin = run_end;
            }
        }
        partition_begin = partition_end;
        group = 0;
    }

    return peer_group_indexes;
}

/// The first row of the block is compared with the last row of the previous block.
std::vector<bool> markGroupStarts(const std::vector<int64_t> & group_indexes, const std::vector<bool> & partition_starts, int64_t last_group_index)
{
    std::vector<bool> starts = partition_starts;
    for (size_t row = 0; row < starts.size(); ++row)
        starts[row] = starts[row] || group_indexes[row] != (row == 0 ? last_group_index : group_indexes[row - 1]);

    return starts;
}

Columns cutLastKey(const Columns & columns, size_t rows_count, const std::vector<size_t> & key_indices)
{
    Columns last_row(columns.size());
    for (const size_t key : key_indices)
        last_row[key] = columns[key]->cut(rows_count - 1, 1);

    return last_row;
}

}

SlidingIndexes::SlidingIndexes(const WindowTransformParams & params_)
    : params(params_)
    , has_order_by_collation(std::ranges::any_of(params.window_description.order_by, [](const SortColumnDescription & column) { return column.collator != nullptr; }))
{
}

SlidingIndex SlidingIndexes::calculate(
    const Columns & materialized_columns, int64_t rows_count, std::vector<bool> & order_by_peer_group_starts)
{
    auto partition_starts = markPartitionStarts(materialized_columns, rows_count, params.partition_by_indices, last_partition_key);
    last_partition_key = cutLastKey(materialized_columns, rows_count, params.partition_by_indices);

    const bool every_row_is_a_group = params.window_description.frame.type == WindowFrame::FrameType::ROWS;
    auto peer_group_indexes = assignPeerGroupIndexes(
        materialized_columns, rows_count, params, every_row_is_a_group, /*compare_with_collation=*/false, partition_starts, last_order_key, last_peer_group_index);
    last_peer_group_index = peer_group_indexes.back();

    if (params.separate_order_by_peer_groups)
    {
        const auto order_by_peer_group_indexes = assignPeerGroupIndexes(
            materialized_columns, rows_count, params, /*every_row_is_a_group=*/false, has_order_by_collation, partition_starts, last_order_key, last_order_by_peer_group_index);
        order_by_peer_group_starts = markGroupStarts(order_by_peer_group_indexes, partition_starts, last_order_by_peer_group_index);
        last_order_by_peer_group_index = order_by_peer_group_indexes.back();
    }
    last_order_key = cutLastKey(materialized_columns, rows_count, params.order_by_indices);

    return SlidingIndex{
        .partition_starts = std::move(partition_starts),
        .peer_group_indexes = std::move(peer_group_indexes),
    };
}

}
