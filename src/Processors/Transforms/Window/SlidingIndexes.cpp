#include <Processors/Transforms/Window/SlidingIndexes.h>

#include <Core/SortCursor.h>

#include <algorithm>
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

void markKeyChangesInRange(const Columns & columns, const std::vector<size_t> & key_indices, size_t begin, size_t end, std::vector<bool> & changes)
{
    size_t next_change = getEqualRangeEndAssumeSorted(columns, key_indices, begin, end, /*nan_direction_hint=*/1);
    while (next_change < end)
    {
        changes[next_change] = true;
        next_change = getEqualRangeEndAssumeSorted(columns, key_indices, next_change, end, /*nan_direction_hint=*/1);
    }
}

std::vector<bool> markPartitionStarts(const Columns & columns, size_t rows_count, const std::vector<size_t> & partition_by_indices, const std::optional<Columns> & previous_partition_key)
{
    std::vector<bool> starts(rows_count, false);
    starts[0] = !previous_partition_key || !haveSameKeys(*previous_partition_key, 0, columns, 0, partition_by_indices);
    markKeyChangesInRange(columns, partition_by_indices, 0, rows_count, starts);
    return starts;
}

std::vector<bool> markOrderByPeerGroupStarts(
    const Columns & columns,
    size_t rows_count,
    const std::vector<size_t> & order_by_indices,
    const std::vector<bool> & partition_starts,
    const std::optional<Columns> & previous_order_key)
{
    if (order_by_indices.empty())
        return partition_starts;

    std::vector<bool> starts = partition_starts;
    starts[0] = starts[0] || !previous_order_key || !haveSameKeys(*previous_order_key, 0, columns, 0, order_by_indices);

    size_t partition_begin = 0;
    while (partition_begin < rows_count)
    {
        const size_t partition_end = std::find(partition_starts.begin() + partition_begin + 1, partition_starts.end(), true) - partition_starts.begin();
        markKeyChangesInRange(columns, order_by_indices, partition_begin, partition_end, starts);
        partition_begin = partition_end;
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
/// rows are compared one by one instead of scanning for runs of equal raw values.
std::vector<bool> markCollatedOrderByPeerGroupStarts(
    const Columns & columns,
    size_t rows_count,
    const WindowTransformParams & params,
    const std::vector<bool> & partition_starts,
    const std::optional<Columns> & previous_order_key)
{
    std::vector<bool> starts = partition_starts;
    starts[0] = starts[0] || !previous_order_key || !haveSameCollatedOrderByKeys(*previous_order_key, 0, columns, 0, params);
    for (size_t row = 1; row < rows_count; ++row)
        starts[row] = starts[row] || !haveSameCollatedOrderByKeys(columns, row - 1, columns, row, params);

    return starts;
}

std::vector<bool> markPeerGroupStarts(
    const Columns & columns,
    size_t rows_count,
    const WindowTransformParams & params,
    const std::vector<bool> & partition_starts,
    const std::optional<Columns> & previous_order_key)
{
    if (params.window_description.frame.type == WindowFrame::FrameType::ROWS)
        return std::vector<bool>(rows_count, true);

    return markOrderByPeerGroupStarts(columns, rows_count, params.order_by_indices, partition_starts, previous_order_key);
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

SlidingIndex SlidingIndexes::calculate(const Columns & materialized_columns, int64_t rows_count)
{
    auto partition_starts = markPartitionStarts(materialized_columns, rows_count, params.partition_by_indices, last_partition_key);
    last_partition_key = cutLastKey(materialized_columns, rows_count, params.partition_by_indices);

    auto peer_group_starts = markPeerGroupStarts(materialized_columns, rows_count, params, partition_starts, last_order_key);
    std::vector<bool> order_by_peer_group_starts;
    if (params.separate_order_by_peer_groups)
        order_by_peer_group_starts = has_order_by_collation
            ? markCollatedOrderByPeerGroupStarts(materialized_columns, rows_count, params, partition_starts, last_order_key)
            : markOrderByPeerGroupStarts(materialized_columns, rows_count, params.order_by_indices, partition_starts, last_order_key);
    last_order_key = cutLastKey(materialized_columns, rows_count, params.order_by_indices);

    return SlidingIndex{
        .partition_starts = std::move(partition_starts),
        .peer_group_starts = std::move(peer_group_starts),
        .order_by_peer_group_starts = std::move(order_by_peer_group_starts),
    };
}

}
