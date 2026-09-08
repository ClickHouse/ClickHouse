#include <Processors/Transforms/LimitByTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <Core/SortCursor.h>
#include <DataTypes/IDataType.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cstdint>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
extern const char limit_by_sorted_stream_transform_pause[];
extern const char limit_by_transform_pause[];
extern const char limit_by_sorted_stream_transform_after_loop_pause[];
extern const char limit_by_transform_after_loop_pause[];
extern const char limit_by_sorted_stream_transform_mid_loop_pause[];
extern const char limit_by_transform_mid_loop_pause[];
}



LimitByTransform::LimitByTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names)
    : ISimpleTransform(header, header, true)
    , group_offset(group_offset_)
    , group_limit_end(computeGroupLimitEnd(group_length_, group_offset_))
    , mapping(*header, column_names)
{
}

void LimitByTransform::processRun(UInt64 run_start_row, UInt64 run_row_count, UInt64 group_rows_seen_before_run)
{
    if (group_rows_seen_before_run >= group_limit_end)
        return;

    const auto slice = shrinkRunToLimitWindow(run_start_row, run_row_count, group_rows_seen_before_run, group_offset, group_limit_end);

    if (slice.length > 0)
        output_slices.push_back(slice);
}

void LimitByTransform::transform(Chunk & chunk)
{
    /// `output_slices` is a member scratch buffer reused across chunks. A previous call may
    /// have thrown after populating it (for example MEMORY_LIMIT_EXCEEDED while the grouping
    /// hash table grows), and `ISimpleTransform::work` keeps this transform alive and calls
    /// it again on the next chunk, so always start from an empty buffer.
    output_slices.clear();

    const UInt64 row_count = chunk.getNumRows();
    if (row_count == 0)
        return;

    auto chunk_columns = chunk.detachColumns();

    if (isCancelled())
    {
        stopReading();
        return;
    }

    /// `filterNonConstKeys` removed all grouping keys, so every row in this chunk
    /// belongs to one logical group and can be processed as one run.
    if (mapping.isTrivial())
    {
        processRun(0, row_count, trivial_group_rows_seen);
        if (trivial_group_rows_seen < group_limit_end)
            trivial_group_rows_seen += row_count;
    }
    else
    {
        const auto & grouping_key_positions = mapping.getKeys().positions;

        Columns normalized_grouping_key_columns;
        normalized_grouping_key_columns.reserve(grouping_key_positions.size());
        ColumnRawPtrs grouping_key_columns;
        grouping_key_columns.reserve(grouping_key_positions.size());
        for (size_t position : grouping_key_positions)
        {
            normalized_grouping_key_columns.push_back(removeSpecialRepresentations(chunk_columns[position])->convertToFullColumnIfConst());
            grouping_key_columns.push_back(normalized_grouping_key_columns.back().get());
        }

        FailPointInjection::pauseFailPoint(FailPoints::limit_by_transform_pause);

        /// `mapChunk` maps rows to groups and splits the chunk into runs.
        mapping.mapChunk(
            grouping_key_columns,
            row_count,
            [&](UInt64 run_start_row, UInt64 run_row_count, size_t group_idx)
            {
                const UInt64 group_rows_seen_before_run = mapping.getRowsSeen(group_idx);
                processRun(run_start_row, run_row_count, group_rows_seen_before_run);
                if (group_rows_seen_before_run < group_limit_end)
                    mapping.setRowsSeen(group_idx, group_rows_seen_before_run + run_row_count);
            },
            [&](UInt64 row_idx)
            {
                if (isCancelled())
                {
                    LOG_TEST(getLogger("LimitByTransform"), "Cancelled during row processing");
                    stopReading();
                    return false;
                }

                if (row_idx == 5)
                    FailPointInjection::pauseFailPoint(FailPoints::limit_by_transform_mid_loop_pause);

                return true;
            });
    }

    FailPointInjection::pauseFailPoint(FailPoints::limit_by_transform_after_loop_pause);

    if (isCancelled())
    {
        LOG_TEST(getLogger("LimitByTransform"), "Cancelled after processing chunk");
        stopReading();
        return;
    }

    /// No row from this chunk survived `LIMIT BY`.
    if (output_slices.empty())
        return;

    const UInt64 output_row_count = materializeSlicesIntoChunk(chunk, std::move(chunk_columns), row_count, output_slices);

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(output_row_count);
}


LimitBySortedStreamTransform::LimitBySortedStreamTransform(
    SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const SortDescription & sorted_columns_descr)
    : ISimpleTransform(header, header, true)
    , group_offset(group_offset_)
    , group_limit_end(computeGroupLimitEnd(group_length_, group_offset_))
{
    Names key_names;
    key_names.reserve(sorted_columns_descr.size());
    for (const auto & column_description : sorted_columns_descr)
        key_names.push_back(column_description.column_name);
    grouping_key_positions = filterNonConstKeys(*header, key_names).positions;

    previous_chunk_last_grouping_key_columns.reserve(grouping_key_positions.size());
    for (size_t position : grouping_key_positions)
        previous_chunk_last_grouping_key_columns.push_back(header->getByPosition(position).type->createColumn());
}

bool LimitBySortedStreamTransform::firstRowContinuesPreviousChunkGroup(const Columns & chunk_columns) const
{
    for (size_t key_idx = 0; key_idx < chunk_columns.size(); ++key_idx)
    {
        if (chunk_columns[key_idx]->compareAt(0, 0, *previous_chunk_last_grouping_key_columns[key_idx], 1) != 0)
            return false;
    }
    return true;
}

void LimitBySortedStreamTransform::rememberLastGroupingKey(const Columns & chunk_columns, UInt64 row_idx)
{
    for (size_t key_idx = 0; key_idx < chunk_columns.size(); ++key_idx)
    {
        auto & previous_chunk_last_grouping_key_column = previous_chunk_last_grouping_key_columns[key_idx];
        if (!previous_chunk_last_grouping_key_column->empty())
            previous_chunk_last_grouping_key_column->popBack(1);
        previous_chunk_last_grouping_key_column->insertFrom(*chunk_columns[key_idx], row_idx);
    }
}

void LimitBySortedStreamTransform::processRun(UInt64 run_start_row, UInt64 run_row_count)
{
    const UInt64 group_rows_seen_before_run = current_group_rows_seen;
    if (group_rows_seen_before_run >= group_limit_end)
        return;

    const auto slice = shrinkRunToLimitWindow(run_start_row, run_row_count, group_rows_seen_before_run, group_offset, group_limit_end);

    if (slice.length > 0)
        output_slices.push_back(slice);

    current_group_rows_seen = group_rows_seen_before_run + run_row_count;
}

void LimitBySortedStreamTransform::transform(Chunk & chunk)
{
    /// See `LimitByTransform::transform`: a previous call may have thrown after populating
    /// this reused scratch buffer, so start each chunk from empty.
    output_slices.clear();

    const UInt64 row_count = chunk.getNumRows();
    if (row_count == 0)
        return;

    auto chunk_columns = chunk.detachColumns();

    if (isCancelled())
    {
        stopReading();
        return;
    }

    Columns normalized_grouping_key_columns;
    normalized_grouping_key_columns.reserve(grouping_key_positions.size());
    for (size_t position : grouping_key_positions)
        normalized_grouping_key_columns.push_back(removeSpecialRepresentations(chunk_columns[position])->convertToFullColumnIfConst());

    /// Row 0 can continue a group from the previous chunk. If its grouping
    /// key changed across the chunk boundary, start a fresh count for this group.
    /// The holder is empty before the first chunk (and when there are no non-constant
    /// grouping keys), in which case there is no previous key to compare against.
    const bool have_previous_chunk_key
        = !previous_chunk_last_grouping_key_columns.empty() && !previous_chunk_last_grouping_key_columns.front()->empty();
    if (have_previous_chunk_key && !firstRowContinuesPreviousChunkGroup(normalized_grouping_key_columns))
        current_group_rows_seen = 0;

    /// Segment the sorted chunk into maximal runs of rows that share one grouping key. Each run is
    /// one group.
    UInt64 current_run_start_row = 0;

    FailPointInjection::pauseFailPoint(FailPoints::limit_by_sorted_stream_transform_pause);

    size_t run_count = 0;
    while (current_run_start_row < row_count)
    {
        if (isCancelled())
        {
            LOG_TEST(getLogger("LimitBySortedStreamTransform"), "Cancelled during row processing");
            stopReading();
            return;
        }

        if (run_count == 5)
            FailPointInjection::pauseFailPoint(FailPoints::limit_by_sorted_stream_transform_mid_loop_pause);

        const UInt64 run_end = getEqualRangeEndAssumeSorted(normalized_grouping_key_columns, current_run_start_row, row_count, 1);
        processRun(current_run_start_row, run_end - current_run_start_row);

        /// A group boundary inside the chunk resets the per-group counter before the next run.
        if (run_end != row_count)
            current_group_rows_seen = 0;
        current_run_start_row = run_end;
        ++run_count;
    }

    /// Save the last grouping key so the next chunk can detect whether its first
    /// row continues the same group or starts a new one. With no non-constant grouping
    /// keys this is a no-op (nothing to remember).
    rememberLastGroupingKey(normalized_grouping_key_columns, row_count - 1);

    FailPointInjection::pauseFailPoint(FailPoints::limit_by_sorted_stream_transform_after_loop_pause);

    if (isCancelled())
    {
        LOG_TEST(getLogger("LimitBySortedStreamTransform"), "Cancelled after processing runs");
        stopReading();
        return;
    }

    /// No row from this chunk survived.
    if (output_slices.empty())
        return;

    const UInt64 output_row_count = materializeSlicesIntoChunk(chunk, std::move(chunk_columns), row_count, output_slices);

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(output_row_count);
}

}
