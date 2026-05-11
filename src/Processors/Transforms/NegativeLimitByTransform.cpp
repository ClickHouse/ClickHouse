#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Port.h>
#include <Processors/Transforms/NegativeLimitByTransform.h>
#include <base/defines.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Compute the sliding-window size = min(length + offset, max_uint64).
UInt64 computeWindowSize(UInt64 length, UInt64 offset)
{
    if (length > std::numeric_limits<UInt64>::max() - offset)
        return std::numeric_limits<UInt64>::max();
    return length + offset;
}

/// Drop keys whose header-sample column is ColumnConst, since constant columns have the
/// same value on every row and cannot distinguish groups.
struct GroupingKeys
{
    Names names;
    std::vector<size_t> positions;
};

GroupingKeys filterNonConstKeys(const SharedHeader & header, const Names & column_names)
{
    GroupingKeys out;
    out.names.reserve(column_names.size());
    out.positions.reserve(column_names.size());
    for (const auto & name : column_names)
    {
        auto position = header->getPositionByName(name);
        const auto & column = header->getByPosition(position).column;
        if (!(column && isColumnConst(*column)))
        {
            out.names.emplace_back(name);
            out.positions.emplace_back(position);
        }
    }
    return out;
}

AggregateDataPtr groupIndexToMapped(size_t idx)
{
    return reinterpret_cast<AggregateDataPtr>(idx + 1);
}
size_t mappedToGroupIndex(AggregateDataPtr mapped)
{
    return reinterpret_cast<size_t>(mapped) - 1;
}

/// Materialize one slice as its own Chunk.
/// Fast path: if the slice covers the whole source chunk, reuse the columns
/// as-is. Otherwise cut each column.
Chunk materializeSliceToChunkIfNeeded(const ChunkSlice & slice)
{
    const Columns & source = *slice.columns;
    chassert(!source.empty());

    /// The chunk slice already spans the entire chunk; return the whole chunk as-is.
    if (slice.start == 0 && slice.length == source.front()->size())
        return Chunk(Columns(source), slice.length);

    Columns result;
    result.reserve(source.size());
    for (const auto & col : source)
        result.push_back(col->cut(slice.start, slice.length));
    return Chunk(std::move(result), slice.length);
}

void releaseHashTable(AggregatedDataVariants & data)
{
    switch (data.type)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case AggregatedDataVariants::Type::NAME: \
        data.NAME.reset(); \
        break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
        case AggregatedDataVariants::Type::EMPTY:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AggregatedDataVariants type in releaseHashTable");
        case AggregatedDataVariants::Type::without_key:
            break;
    }
    data.invalidate();
    data.aggregates_pools.clear();
    data.aggregates_pool = nullptr;
}
}

NegativeLimitByTransform::NegativeLimitByTransform(
    SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names)
    : IAccumulatingTransform(header, header)
    , group_offset(group_offset_)
    , group_window_size(computeWindowSize(group_length_, group_offset_))
{
    auto grouping_keys = filterNonConstKeys(header, column_names);
    key_positions = std::move(grouping_keys.positions);

    data.keys_size = grouping_keys.names.size();
    auto type = AggregatedDataVariants::chooseMethod(*header, grouping_keys.names, data.key_sizes);
    data.init(type);

    ColumnsHashing::HashMethodContextSettings ctx_settings;
    ctx_settings.max_threads = 1;
    hash_method_context = AggregatedDataVariants::createCache(type, ctx_settings);
}

void NegativeLimitByTransform::dropExcessRows(GroupWindow & window)
{
    while (window.window_rows > group_window_size)
    {
        chassert(!window.slices.empty());
        auto front_slice_it = window.slices.front();
        UInt64 excess = window.window_rows - group_window_size;
        if (front_slice_it->length <= excess) /// Even without the front slice, we have enough rows in the window.
        {
            window.window_rows -= front_slice_it->length;
            candidate_list.erase(front_slice_it);
            window.slices.pop_front();
        }
        else
        {
            /// Trim the front slice.
            front_slice_it->start += excess;
            front_slice_it->length -= excess;
            window.window_rows -= excess;
            break;
        }
    }
}

void NegativeLimitByTransform::dropOffsetRows(GroupWindow & window)
{
    UInt64 to_drop = std::min(group_offset, window.window_rows);
    while (to_drop > 0)
    {
        chassert(!window.slices.empty());
        auto back_slice_it = window.slices.back();
        if (back_slice_it->length <= to_drop)
        {
            to_drop -= back_slice_it->length;
            window.window_rows -= back_slice_it->length;
            candidate_list.erase(back_slice_it);
            window.slices.pop_back();
        }
        else
        {
            back_slice_it->length -= to_drop;
            window.window_rows -= to_drop;
            break;
        }
    }
}

void NegativeLimitByTransform::appendRun(const ChunkSlice::ColumnsPtr & columns_ptr, UInt64 start, UInt64 length, size_t group_idx)
{
    auto & window = group_windows[group_idx];
    auto slice_it = candidate_list.insert(candidate_list.end(), ChunkSlice{columns_ptr, start, length});
    window.slices.push_back(slice_it);
    window.window_rows += length;

    /// Evict older slices if the window now exceeds `group_window_size`.
    dropExcessRows(window);
}

template <typename Method>
void NegativeLimitByTransform::consumeImpl(
    Method & method, const ColumnRawPtrs & key_columns, const ChunkSlice::ColumnsPtr & columns_ptr, UInt64 num_rows)
{
    typename Method::State state(key_columns, data.key_sizes, hash_method_context);

    UInt64 run_start = 0;
    size_t run_group = 0;
    for (UInt64 row = 0; row < num_rows; ++row)
    {
        auto emplace_result = state.emplaceKey(method.data, row, *data.aggregates_pool);
        size_t group_idx;
        if (emplace_result.isInserted()) /// New grouping key
        {
            group_idx = group_windows.size();
            group_windows.emplace_back();
            emplace_result.setMapped(groupIndexToMapped(group_idx));
        }
        else /// Existing grouping key
            group_idx = mappedToGroupIndex(emplace_result.getMapped());

        if (row == 0)
            run_group = group_idx;
        else if (group_idx != run_group) /// Local run ended
        {
            appendRun(columns_ptr, run_start, row - run_start, run_group);
            run_group = group_idx;
            run_start = row;
        }
    }
    appendRun(columns_ptr, run_start, num_rows - run_start, run_group);
}

void NegativeLimitByTransform::consume(Chunk chunk)
{
    UInt64 num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto columns_ptr = std::make_shared<const Columns>(chunk.detachColumns());
    const auto & cols = *columns_ptr;

    /// Degenerate case: all grouping keys were constant (filtered out via `filterNonConstKeys`) -
    /// every row is part of the same group.
    if (data.type == AggregatedDataVariants::Type::without_key)
    {
        if (group_windows.empty())
            group_windows.emplace_back();
        appendRun(columns_ptr, 0, num_rows, 0);
        return;
    }

    ColumnRawPtrs key_columns;
    key_columns.reserve(key_positions.size());
    for (size_t pos : key_positions)
        key_columns.push_back(cols[pos].get());

    switch (data.type)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case AggregatedDataVariants::Type::NAME: \
        consumeImpl(*data.NAME, key_columns, columns_ptr, num_rows); \
        break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

        case AggregatedDataVariants::Type::EMPTY:
        case AggregatedDataVariants::Type::without_key:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AggregatedDataVariants type in NegativeLimitByTransform::consume");
    }
}

Chunk NegativeLimitByTransform::generate()
{
    if (!offset_rows_dropped)
    {
        /// Drop trailing `offset` rows per group, then release the hash table and per-group
        /// state - from here on `candidate_list` alone carries the answer in input order.
        for (auto & window : group_windows)
            dropOffsetRows(window);
        offset_rows_dropped = true;
        releaseHashTable(data);
        hash_method_context.reset();
        std::vector<GroupWindow>{}.swap(group_windows);
    }

    if (candidate_list.empty())
        return {};

    /// At this point, `candidate_list` contains all the rows that will be part of the output.
    /// Per grouping key we have at most `group_length` rows. `candidate_list` holds the data in
    /// input order, so we can emit from the head of the list.

    /// Coalesce consecutive slices sharing one source chunk into a single output chunk.
    /// This avoids emitting many tiny chunks when the grouping key has high cardinality.
    auto coalesce_slice_begin = candidate_list.begin();
    auto columns_ptr = coalesce_slice_begin->columns;
    auto coalesce_slice_end = std::next(coalesce_slice_begin);
    while (coalesce_slice_end != candidate_list.end() && coalesce_slice_end->columns == columns_ptr)
        ++coalesce_slice_end;

    Chunk chunk;
    if (std::next(coalesce_slice_begin) == coalesce_slice_end) /// Only one slice in the chunk
    {
        chunk = materializeSliceToChunkIfNeeded(*coalesce_slice_begin);
    }
    else
    {
        const Columns & source = *columns_ptr;
        UInt64 chunk_size = source.front()->size();

        /// Compute some statistics to pick an optimized materialization strategy below.
        UInt64 num_output_rows = 0;
        UInt64 first_slice_start_row = coalesce_slice_begin->start;
        UInt64 last_slice_end_row = 0;
        for (auto it = coalesce_slice_begin; it != coalesce_slice_end; ++it)
        {
            num_output_rows += it->length;
            last_slice_end_row = it->start + it->length;
        }

        if (num_output_rows == chunk_size)
        {
            /// Every row of the source chunk survived across different keys.
            chunk = Chunk(Columns(source), num_output_rows);
        }
        else if (last_slice_end_row - first_slice_start_row == num_output_rows)
        {
            /// Slices form one contiguous segment - single cut per column, which is more efficient than mask+filter.
            Columns result;
            result.reserve(source.size());
            for (const auto & col : source)
                result.push_back(col->cut(first_slice_start_row, num_output_rows));
            chunk = Chunk(std::move(result), num_output_rows);
        }
        else
        {
            /// Sparse kept rows within the chunk: OR all slice ranges into one
            /// mask and filter.
            IColumn::Filter mask(chunk_size, 0);
            for (auto it = coalesce_slice_begin; it != coalesce_slice_end; ++it)
                std::fill_n(mask.begin() + it->start, it->length, UInt8{1});

            Columns result;
            result.reserve(source.size());
            for (const auto & col : source)
                result.push_back(isColumnConst(*col) ? col->cut(0, num_output_rows) : col->filter(mask, num_output_rows));
            chunk = Chunk(std::move(result), num_output_rows);
        }
    }

    candidate_list.erase(coalesce_slice_begin, coalesce_slice_end);
    return chunk;
}


NegativeLimitBySortedStreamTransform::NegativeLimitBySortedStreamTransform(
    SharedHeader header, UInt64 group_length_, UInt64 group_offset_, const Names & column_names)
    : IInflatingTransform(header, header)
    , key_positions(filterNonConstKeys(header, column_names).positions)
    , group_offset(group_offset_)
    , group_window_size(computeWindowSize(group_length_, group_offset_))
{
}

bool NegativeLimitBySortedStreamTransform::sameAsPrevChunkKey(const Columns & cols, UInt64 row) const
{
    for (size_t i = 0; i < key_positions.size(); ++i)
    {
        if (cols[key_positions[i]]->compareAt(row, 0, *prev_key_columns[i], 1) != 0)
            return false;
    }
    return true;
}

bool NegativeLimitBySortedStreamTransform::sameAsRowBefore(const Columns & cols, UInt64 row) const
{
    for (auto pos : key_positions)
    {
        const auto & col = *cols[pos];
        if (col.compareAt(row, row - 1, col, 1) != 0)
            return false;
    }
    return true;
}

void NegativeLimitBySortedStreamTransform::rememberKey(const Columns & cols, UInt64 row)
{
    if (key_positions.empty())
        return;

    if (prev_key_columns.empty())
    {
        prev_key_columns.reserve(key_positions.size());
        for (auto pos : key_positions)
            prev_key_columns.emplace_back(cols[pos]->cloneEmpty());
    }

    for (size_t i = 0; i < key_positions.size(); ++i)
    {
        if (!prev_key_columns[i]->empty())
            prev_key_columns[i]->popBack(prev_key_columns[i]->size());
        prev_key_columns[i]->insertFrom(*cols[key_positions[i]], row);
    }
}

/// Ensure `GroupWindow` does not track more than `group_window_size` rows.
void NegativeLimitBySortedStreamTransform::dropExcessRows(GroupWindow & window) const
{
    while (window.window_rows > group_window_size)
    {
        chassert(!window.slices.empty());
        auto & front = window.slices.front();
        UInt64 excess = window.window_rows - group_window_size;
        if (front.length <= excess) /// Even without the front slice, we have enough rows in the window.
        {
            window.window_rows -= front.length;
            window.slices.pop_front();
        }
        else
        {
            /// Trim the front slice.
            front.start += excess;
            front.length -= excess;
            window.window_rows -= excess;
            break;
        }
    }
}

/// Called once the current group is complete (all its rows are in the window) - removes
/// the trailing `group_offset` rows to finalize the group's output.
void NegativeLimitBySortedStreamTransform::dropOffsetRows(GroupWindow & window) const
{
    UInt64 to_drop = std::min(group_offset, window.window_rows);
    while (to_drop > 0)
    {
        chassert(!window.slices.empty());
        auto & back = window.slices.back();
        if (back.length <= to_drop) /// Entire back slice is within the OFFSET rows to drop.
        {
            to_drop -= back.length;
            window.window_rows -= back.length;
            window.slices.pop_back();
        }
        else
        {
            back.length -= to_drop;
            window.window_rows -= to_drop;
            break;
        }
    }
}

void NegativeLimitBySortedStreamTransform::finalizeWindow(GroupWindow & window)
{
    dropOffsetRows(window);
    for (const auto & slice : window.slices)
    {
        chassert(slice.length > 0);
        pending.push_back(slice);
    }
    window = {};
}

void NegativeLimitBySortedStreamTransform::consume(Chunk chunk)
{
    UInt64 num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto columns_ptr = std::make_shared<const Columns>(chunk.detachColumns());
    const auto & cols = *columns_ptr;

    auto append_run = [&](UInt64 start, UInt64 length)
    {
        current_group_window.slices.push_back({columns_ptr, start, length});
        current_group_window.window_rows += length;
        dropExcessRows(current_group_window);
    };

    UInt64 run_start = 0;

    /// Row 0 is the only row that needs `prev_key_columns` - it sits on the chunk boundary.
    /// If the key changes at the boundary, the previous chunk's group will never be seen
    /// again and we can safely finalize it.
    if (!prev_key_columns.empty() && !sameAsPrevChunkKey(cols, 0))
        finalizeWindow(current_group_window);

    for (UInt64 row = 1; row < num_rows; ++row)
    {
        if (sameAsRowBefore(cols, row))
            continue;

        /// Group boundary: close the current run, finalize, start a new run.
        append_run(run_start, row - run_start);
        finalizeWindow(current_group_window);
        run_start = row;
    }

    append_run(run_start, num_rows - run_start);

    /// Remember this chunk's last row for the next chunk's boundary check.
    rememberKey(cols, num_rows - 1);
}

bool NegativeLimitBySortedStreamTransform::canGenerate()
{
    return !pending.empty();
}

Chunk NegativeLimitBySortedStreamTransform::generate()
{
    chassert(!pending.empty());

    /// Coalesce consecutive slices sharing one source chunk into a single output chunk.
    /// This avoids emitting many tiny chunks when the grouping key has high cardinality.
    auto columns_ptr = pending.front().columns;

    /// Compute some statistics to pick an optimized materialization strategy below.
    UInt64 num_output_rows = 0;
    UInt64 first_slice_start_row = pending.front().start;
    UInt64 last_slice_end_row = 0;
    size_t num_slices_to_coalesce = 0;
    for (const auto & slice : pending)
    {
        if (slice.columns != columns_ptr)
            break;
        num_output_rows += slice.length;
        last_slice_end_row = slice.start + slice.length;
        ++num_slices_to_coalesce;
    }

    const Columns & source = *columns_ptr;
    UInt64 chunk_size = source.front()->size();
    Chunk chunk;

    if (num_slices_to_coalesce == 1)
    {
        chunk = materializeSliceToChunkIfNeeded(pending.front());
    }
    else if (num_output_rows == chunk_size)
    {
        /// Every row of the source chunk survived across different keys.
        chunk = Chunk(Columns(source), num_output_rows);
    }
    else if (last_slice_end_row - first_slice_start_row == num_output_rows)
    {
        /// Slices form one contiguous segment - single cut per column, which is more efficient than mask+filter.
        Columns result;
        result.reserve(source.size());
        for (const auto & col : source)
            result.push_back(col->cut(first_slice_start_row, num_output_rows));
        chunk = Chunk(std::move(result), num_output_rows);
    }
    else
    {
        /// Sparse kept rows within the chunk: OR all slice ranges into one
        /// mask and filter.
        IColumn::Filter mask(chunk_size, 0);
        for (size_t i = 0; i < num_slices_to_coalesce; ++i)
        {
            const auto & slice = pending[i];
            std::fill_n(mask.begin() + slice.start, slice.length, UInt8{1});
        }

        Columns result;
        result.reserve(source.size());
        for (const auto & col : source)
            result.push_back(isColumnConst(*col) ? col->cut(0, num_output_rows) : col->filter(mask, num_output_rows));
        chunk = Chunk(std::move(result), num_output_rows);
    }

    pending.erase(pending.begin(), pending.begin() + num_slices_to_coalesce);
    return chunk;
}

Chunk NegativeLimitBySortedStreamTransform::getRemaining()
{
    finalizeWindow(current_group_window);
    if (pending.empty())
        return {};
    Chunk chunk = generate();

    /// Remaining pending slices drain through canGenerate()/generate()
    can_generate = !pending.empty();
    return chunk;
}

}
