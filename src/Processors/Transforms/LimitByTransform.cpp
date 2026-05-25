#include <Processors/Transforms/LimitByTransform.h>

#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <base/defines.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cstdint>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Compute the exclusive end of the kept per-group interval `[offset, offset + length)`.
UInt64 computeGroupLimitEnd(UInt64 length, UInt64 offset)
{
    if (length > std::numeric_limits<UInt64>::max() - offset)
        return std::numeric_limits<UInt64>::max();
    return length + offset;
}

struct GroupingKeys
{
    Names names;
    std::vector<size_t> positions;
};

/// Collect grouping keys whose header-sample column is not `ColumnConst`.
/// Constant columns do not help distinguish groups because they have the same value on every row.
GroupingKeys filterNonConstKeys(const SharedHeader & header, const Names & column_names)

{
    GroupingKeys non_const_keys;
    non_const_keys.names.reserve(column_names.size());
    non_const_keys.positions.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        auto position = header->getPositionByName(column_name);
        const auto & column = header->getByPosition(position).column;
        if (!(column && isColumnConst(*column)))
        {
            non_const_keys.names.emplace_back(column_name);
            non_const_keys.positions.emplace_back(position);
        }
    }
    return non_const_keys;
}

AggregateDataPtr mappedFromGroupIndex(size_t group_index)
{
    return reinterpret_cast<AggregateDataPtr>(static_cast<uintptr_t>(group_index) + 1);
}

size_t groupIndexFromMapped(AggregateDataPtr mapped)
{
    return static_cast<size_t>(reinterpret_cast<uintptr_t>(mapped) - 1);
}

/// Return the chunk-local portion of a same-group run that falls into
/// the kept per-group interval `[group_offset, group_limit_end)`.
/// `run_start_row` and `run_row_count` describe the run inside the current chunk.
/// `group_rows_seen_before_run` is the total number of rows already seen for this
/// group before the run starts. `length == 0` means the run contributes no rows.
ChunkSlice shrinkRunToLimitWindow(
    UInt64 run_start_row, UInt64 run_row_count, UInt64 group_rows_seen_before_run, UInt64 group_offset, UInt64 group_limit_end)
{
    /// Rows from this run consumed by the group's OFFSET prefix.
    const UInt64 offset_rows_in_run
        = group_rows_seen_before_run < group_offset ? std::min(group_offset - group_rows_seen_before_run, run_row_count) : 0;

    /// The group's row count after skipping the OFFSET-covered prefix of this run.
    const UInt64 group_rows_seen_after_offset = group_rows_seen_before_run + offset_rows_in_run;

    /// Remaining rows this group can still contribute before reaching the limit end.
    const UInt64 remaining_rows_until_limit_end
        = group_rows_seen_after_offset < group_limit_end ? group_limit_end - group_rows_seen_after_offset : 0;

    const UInt64 rows_kept_from_run = std::min(run_row_count - offset_rows_in_run, remaining_rows_until_limit_end);
    return {run_start_row + offset_rows_in_run, rows_kept_from_run};
}

/// Materialize chunk-local `slices` from one source `Columns` into `chunk` and
/// return the output row count. `slices` must be non-empty, ordered by `start`,
/// non-overlapping, and each slice must stay within `[0, source_row_count)`.
/// Reuse the whole chunk when possible; otherwise prefer a single `cut` for
/// contiguous rows and fall back to mask-based `filter`.
UInt64 materializeSlicesIntoChunk(Chunk & chunk, Columns && source_columns, UInt64 source_row_count, const std::vector<ChunkSlice> & slices)
{
    UInt64 output_row_count = 0;
    for (const auto & slice : slices)
        output_row_count += slice.length;

    const UInt64 first_slice_start = slices.front().start;
    const UInt64 last_slice_end = slices.back().start + slices.back().length;

    if (slices.size() == 1)
    {
        const auto & slice = slices.front();

        /// A single slice keeps the whole chunk, so reuse the source columns.
        if (slice.length == source_row_count)
        {
            chassert(slice.start == 0);
            chunk.setColumns(std::move(source_columns), slice.length);
            return output_row_count;
        }

        Columns output_columns;
        output_columns.reserve(source_columns.size());
        for (const auto & column : source_columns)
            output_columns.push_back(column->cut(slice.start, slice.length));
        chunk.setColumns(std::move(output_columns), slice.length);
        return output_row_count;
    }

    if (output_row_count == source_row_count)
    {
        /// All rows survived, but as multiple slices. Reuse the source columns.
        chunk.setColumns(std::move(source_columns), output_row_count);
        return output_row_count;
    }

    /// Because `slices` are ordered and non-overlapping, if the span from the
    /// first slice start to the last slice end has the same length as the sum
    /// of slice lengths, then the slices have no gaps and form one segment.
    if (last_slice_end - first_slice_start == output_row_count)
    {
        Columns output_columns;
        output_columns.reserve(source_columns.size());
        for (const auto & column : source_columns)
            output_columns.push_back(column->cut(first_slice_start, output_row_count));
        chunk.setColumns(std::move(output_columns), output_row_count);
        return output_row_count;
    }

    /// Kept rows are sparse within the chunk, so build one mask and `filter`.
    IColumn::Filter mask(source_row_count, 0);
    for (const auto & slice : slices)
        std::fill_n(mask.begin() + slice.start, slice.length, UInt8{1});

    Columns output_columns;
    output_columns.reserve(source_columns.size());
    /// For `ColumnConst`, `filter` would work too, but it would scan the mask
    /// again to count selected rows. We already know `output_row_count`, so use `cut`.
    for (const auto & column : source_columns)
        output_columns.push_back(isColumnConst(*column) ? column->cut(0, output_row_count) : column->filter(mask, output_row_count));
    chunk.setColumns(std::move(output_columns), output_row_count);
    return output_row_count;
}

}


LimitByTransform::LimitByTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names)
    : ISimpleTransform(header, header, true)
    , group_offset(group_offset_)
    , group_limit_end(computeGroupLimitEnd(group_length_, group_offset_))
{
    auto grouping_keys = filterNonConstKeys(header, column_names);
    grouping_key_positions = std::move(grouping_keys.positions);

    data.keys_size = grouping_keys.names.size();
    auto type = AggregatedDataVariants::chooseMethod(*header, grouping_keys.names, data.key_sizes);
    data.init(type);

    ColumnsHashing::HashMethodContextSettings ctx_settings;
    ctx_settings.max_threads = 1;
    hash_method_context = AggregatedDataVariants::createCache(type, ctx_settings);
}

void LimitByTransform::processRun(UInt64 run_start_row, UInt64 run_row_count, size_t group_idx)
{
    const UInt64 group_rows_seen_before_run = group_counts[group_idx];
    if (group_rows_seen_before_run >= group_limit_end)
        return;

    const auto slice = shrinkRunToLimitWindow(run_start_row, run_row_count, group_rows_seen_before_run, group_offset, group_limit_end);

    if (slice.length > 0)
        output_slices.push_back(slice);

    group_counts[group_idx] = group_rows_seen_before_run + run_row_count;
}

template <typename Method>
void LimitByTransform::consumeImpl(Method & hash_method, const ColumnRawPtrs & grouping_key_columns, UInt64 row_count)
{
    typename Method::State state(grouping_key_columns, data.key_sizes, hash_method_context);

    UInt64 current_run_start_row = 0;
    size_t current_run_group_idx = 0;
    for (UInt64 row_idx = 0; row_idx < row_count; ++row_idx)
    {
        auto key_emplace_result = state.emplaceKey(hash_method.data, row_idx, *data.aggregates_pool);
        size_t row_group_idx;
        if (key_emplace_result.isInserted()) /// New grouping key
        {
            /// Assign a stable index into `group_counts` to the grouping key we just inserted.
            row_group_idx = group_counts.size();
            group_counts.push_back(0);
            key_emplace_result.setMapped(mappedFromGroupIndex(row_group_idx));
        }
        else /// Existing grouping key
            row_group_idx = groupIndexFromMapped(key_emplace_result.getMapped());

        if (row_idx == 0)
            current_run_group_idx = row_group_idx;
        else if (row_group_idx != current_run_group_idx) /// Local run ended
        {
            processRun(current_run_start_row, row_idx - current_run_start_row, current_run_group_idx);
            current_run_group_idx = row_group_idx;
            current_run_start_row = row_idx;
        }
    }

    /// Flush the final run, which extends to the end of the chunk.
    processRun(current_run_start_row, row_count - current_run_start_row, current_run_group_idx);
}

void LimitByTransform::transform(Chunk & chunk)
{
    const UInt64 row_count = chunk.getNumRows();
    if (row_count == 0)
        return;

    auto chunk_columns = chunk.detachColumns();

    /// `filterNonConstKeys` removed all grouping keys, so every row in this chunk
    /// belongs to one logical group and can be processed as one run.
    if (data.type == AggregatedDataVariants::Type::without_key)
    {
        if (group_counts.empty())
            group_counts.push_back(0);
        processRun(0, row_count, 0);
    }
    else
    {
        Columns normalized_grouping_key_columns;
        normalized_grouping_key_columns.reserve(grouping_key_positions.size());
        ColumnRawPtrs grouping_key_columns;
        grouping_key_columns.reserve(grouping_key_positions.size());
        for (size_t position : grouping_key_positions)
        {
            normalized_grouping_key_columns.push_back(removeSpecialRepresentations(chunk_columns[position]));
            grouping_key_columns.push_back(normalized_grouping_key_columns.back().get());
        }

        /// `consumeImpl` maps rows to groups and splits the chunk into runs.
        switch (data.type)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case AggregatedDataVariants::Type::NAME: \
        consumeImpl(*data.NAME, grouping_key_columns, row_count); \
        break;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

            case AggregatedDataVariants::Type::EMPTY:
            case AggregatedDataVariants::Type::without_key:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AggregatedDataVariants type in LimitByTransform::transform");
        }
    }

    /// No row from this chunk survived `LIMIT BY`.
    if (output_slices.empty())
        return;

    const UInt64 output_row_count = materializeSlicesIntoChunk(chunk, std::move(chunk_columns), row_count, output_slices);
    output_slices.clear();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(output_row_count);
}


LimitBySortedStreamTransform::LimitBySortedStreamTransform(
    SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names)
    : ISimpleTransform(header, header, true)
    , grouping_key_positions(filterNonConstKeys(header, column_names).positions)
    , group_offset(group_offset_)
    , group_limit_end(computeGroupLimitEnd(group_length_, group_offset_))
{
    previous_chunk_last_grouping_key_columns.reserve(grouping_key_positions.size());
    for (size_t position : grouping_key_positions)
        previous_chunk_last_grouping_key_columns.emplace_back(header->getByPosition(position).type->createColumn());
}

bool LimitBySortedStreamTransform::firstRowContinuesPreviousChunkGroup(const Columns & chunk_columns) const
{
    for (size_t key_idx = 0; key_idx < grouping_key_positions.size(); ++key_idx)
    {
        if (chunk_columns[grouping_key_positions[key_idx]]->compareAt(0, 0, *previous_chunk_last_grouping_key_columns[key_idx], 1) != 0)
            return false;
    }
    return true;
}

bool LimitBySortedStreamTransform::hasSameGroupingKeyAsPreviousRow(const Columns & chunk_columns, UInt64 row_idx) const
{
    for (size_t position : grouping_key_positions)
    {
        const auto & column = *chunk_columns[position];
        if (column.compareAt(row_idx, row_idx - 1, column, 1) != 0)
            return false;
    }
    return true;
}

void LimitBySortedStreamTransform::rememberLastGroupingKey(const Columns & chunk_columns, UInt64 row_idx)
{
    for (size_t key_idx = 0; key_idx < grouping_key_positions.size(); ++key_idx)
    {
        auto & previous_chunk_last_grouping_key_column = previous_chunk_last_grouping_key_columns[key_idx];
        if (!previous_chunk_last_grouping_key_column->empty())
            previous_chunk_last_grouping_key_column->popBack(1);
        previous_chunk_last_grouping_key_column->insertFrom(*chunk_columns[grouping_key_positions[key_idx]], row_idx);
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
    const UInt64 row_count = chunk.getNumRows();
    if (row_count == 0)
        return;

    auto chunk_columns = chunk.detachColumns();

    /// Row 0 can continue a group from the previous chunk. If its grouping
    /// key changed across the chunk boundary, start a fresh count for this group.
    /// Skip the comparison on the very first chunk (no key stored yet) and when
    /// all keys are constant (vector is empty).
    if (!previous_chunk_last_grouping_key_columns.empty()
        && !previous_chunk_last_grouping_key_columns[0]->empty()
        && !firstRowContinuesPreviousChunkGroup(chunk_columns))
        current_group_rows_seen = 0;

    UInt64 current_run_start_row = 0;
    for (UInt64 row_idx = 1; row_idx < row_count; ++row_idx)
    {
        if (hasSameGroupingKeyAsPreviousRow(chunk_columns, row_idx))
            continue;

        /// The grouping key changed within the chunk, so finish the current run
        /// and reset the counter before starting the next group's run.
        processRun(current_run_start_row, row_idx - current_run_start_row);
        current_group_rows_seen = 0;
        current_run_start_row = row_idx;
    }

    /// Flush the final run, which extends to the end of the chunk.
    processRun(current_run_start_row, row_count - current_run_start_row);

    /// Save the last grouping key so the next chunk can detect whether its first
    /// row continues the same group or starts a new one.
    rememberLastGroupingKey(chunk_columns, row_count - 1);

    /// No row from this chunk survived.
    if (output_slices.empty())
        return;

    const UInt64 output_row_count = materializeSlicesIntoChunk(chunk, std::move(chunk_columns), row_count, output_slices);
    output_slices.clear();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(output_row_count);
}

}
