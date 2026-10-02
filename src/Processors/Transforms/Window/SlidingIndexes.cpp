#include <Processors/Transforms/Window/SlidingIndexes.h>

#include <Core/SortCursor.h>

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

std::vector<bool> markKeyChanges(const Columns & columns, size_t rows_count, const std::vector<size_t> & key_indices, const std::optional<Columns> & previous_key)
{
    std::vector<bool> changes(rows_count, false);
    changes[0] = !previous_key || !haveSameKeys(*previous_key, 0, columns, 0, key_indices);

    size_t next_change = getEqualRangeEndAssumeSorted(columns, key_indices, 0, rows_count, /*nan_direction_hint=*/1);
    while (next_change < rows_count)
    {
        changes[next_change] = true;
        next_change = getEqualRangeEndAssumeSorted(columns, key_indices, next_change, rows_count, /*nan_direction_hint=*/1);
    }

    return changes;
}

std::vector<bool> markPeerGroupStarts(const Columns & columns, size_t rows_count, const WindowTransformParams & params, const std::optional<Columns> & previous_key)
{
    if (params.window_description.frame.type == WindowFrame::FrameType::ROWS)
        return std::vector<bool>(rows_count, true);

    return markKeyChanges(columns, rows_count, params.peer_key_indices, previous_key);
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
    auto partition_starts = markKeyChanges(materialized_columns, rows_count, params.partition_by_indices, last_partition_key);
    last_partition_key = cutLastKey(materialized_columns, rows_count, params.partition_by_indices);

    auto peer_group_starts = markPeerGroupStarts(materialized_columns, rows_count, params, last_peer_key);
    last_peer_key = cutLastKey(materialized_columns, rows_count, params.peer_key_indices);

    return SlidingIndex{
        .partition_starts = std::move(partition_starts),
        .peer_group_starts = std::move(peer_group_starts),
    };
}

}
