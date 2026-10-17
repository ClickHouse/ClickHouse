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

std::vector<int64_t> assignPeerGroupIndexes(
    const Columns & columns,
    size_t rows_count,
    const WindowTransformParams & params,
    const std::vector<bool> & partition_starts,
    const std::optional<Columns> & previous_order_key,
    int64_t last_peer_group_index)
{
    std::vector<int64_t> peer_group_indexes(rows_count);
    const std::vector<size_t> & order_by_indices = params.order_by_indices;
    const bool every_row_is_a_group = params.window_description.frame.type == WindowFrame::FrameType::ROWS;
    if (!every_row_is_a_group && order_by_indices.empty())
        return peer_group_indexes;

    const bool first_row_continues_last_group = !every_row_is_a_group && previous_order_key && params.haveEqualOrderByValues(*previous_order_key, 0, columns, 0);
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
                const size_t run_end = params.getOrderByEqualRangeEnd(columns, run_begin, partition_end);
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
{
}

SlidingIndex SlidingIndexes::calculate(const Columns & materialized_columns, int64_t rows_count)
{
    auto partition_starts = markPartitionStarts(materialized_columns, rows_count, params.partition_by_indices, last_partition_key);
    last_partition_key = cutLastKey(materialized_columns, rows_count, params.partition_by_indices);

    auto peer_group_indexes = assignPeerGroupIndexes(materialized_columns, rows_count, params, partition_starts, last_order_key, last_peer_group_index);
    last_order_key = cutLastKey(materialized_columns, rows_count, params.order_by_indices);
    last_peer_group_index = peer_group_indexes.back();

    return SlidingIndex{
        .partition_starts = std::move(partition_starts),
        .peer_group_indexes = std::move(peer_group_indexes),
    };
}

}
