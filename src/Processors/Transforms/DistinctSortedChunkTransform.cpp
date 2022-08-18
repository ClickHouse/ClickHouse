#include <Processors/Transforms/DistinctSortedChunkTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctSortedChunkTransform::DistinctSortedChunkTransform(
    const Block & header_,
    const SizeLimits & output_size_limits_,
    UInt64 limit_hint_,
    const SortDescription & sorted_columns_descr_,
    const Names & source_columns,
    const bool sorted_stream_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , output_size_limits(output_size_limits_)
    , sorted_columns_descr(sorted_columns_descr_)
    , sorted_stream(sorted_stream_)
{
    /// calculate sorted columns positions
    sorted_columns_pos.reserve(sorted_columns_descr.size());
    for (auto const & descr : sorted_columns_descr)
    {
        size_t pos = header_.getPositionByName(descr.column_name);
        sorted_columns_pos.emplace_back(pos);
    }

    /// calculate non-sorted columns positions
    other_columns_pos.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
    {
        size_t pos = header_.getPositionByName(source_column);
        if (std::find(sorted_columns_pos.begin(), sorted_columns_pos.end(), pos) != sorted_columns_pos.end())
            continue;

        const auto & col = header_.getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            other_columns_pos.emplace_back(pos);
    }

    /// reserve space in auxiliary column vectors for processing
    sorted_columns.reserve(sorted_columns_pos.size());
    other_columns.reserve(other_columns_pos.size());
    prev_chunk_latest_key.reserve(sorted_columns.size());
}

void DistinctSortedChunkTransform::initChunkProcessing(const Columns & input_columns)
{
    sorted_columns.clear();
    for (size_t pos : sorted_columns_pos)
        sorted_columns.emplace_back(input_columns[pos].get());

    other_columns.clear();
    for (size_t pos : other_columns_pos)
        other_columns.emplace_back(input_columns[pos].get());

    if (!other_columns.empty() && data.type == ClearableSetVariants::Type::EMPTY)
        data.init(ClearableSetVariants::chooseMethod(other_columns, other_columns_sizes));
}

template <bool clear_data>
size_t DistinctSortedChunkTransform::ordinaryDistinctOnRange(IColumn::Filter & filter, const size_t range_begin, const size_t range_end)
{
    size_t count = 0;
    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
            // clang-format off
#define M(NAME) \
        case ClearableSetVariants::Type::NAME: \
            if constexpr (clear_data) data.NAME->data.clear(); \
            count = buildFilterForRange(*data.NAME, filter, range_begin, range_end); \
            break;

        APPLY_FOR_SET_VARIANTS(M)
#undef M
            // clang-format on
    }
    return count;
}

template <typename Method>
size_t DistinctSortedChunkTransform::buildFilterForRange(
    Method & method, IColumn::Filter & filter, const size_t range_begin, const size_t range_end)
{
    typename Method::State state(other_columns, other_columns_sizes, nullptr);

    size_t count = 0;
    for (size_t i = range_begin; i < range_end; ++i)
    {
        const auto emplace_result = state.emplaceKey(method.data, i, data.string_pool);

        /// emit the record if there is no such key in the current set, skip otherwise
        filter[i] = emplace_result.isInserted();
        if (emplace_result.isInserted())
            ++count;
    }
    return count;
}

void DistinctSortedChunkTransform::saveLatestKey(const size_t row_pos)
{
    prev_chunk_latest_key.clear();
    for (const auto & col : sorted_columns)
    {
        prev_chunk_latest_key.emplace_back(col->cloneEmpty());
        prev_chunk_latest_key.back()->insertFrom(*col, row_pos);
    }
}

bool DistinctSortedChunkTransform::isKey(const size_t key_pos, const size_t row_pos) const
{
    for (size_t i = 0; i < sorted_columns.size(); ++i)
    {
        const int res = sorted_columns[i]->compareAt(key_pos, row_pos, *sorted_columns[i], sorted_columns_descr[i].nulls_direction);
        if (res != 0)
            return false;
    }
    return true;
}

bool DistinctSortedChunkTransform::isLatestKeyFromPrevChunk(const size_t row_pos) const
{
    for (size_t i = 0; i < sorted_columns.size(); ++i)
    {
        const int res = prev_chunk_latest_key[i]->compareAt(0, row_pos, *sorted_columns[i], sorted_columns_descr[i].nulls_direction);
        if (res != 0)
            return false;
    }
    return true;
}

template<typename Predicate>
size_t DistinctSortedChunkTransform::getRangeEnd(size_t begin, size_t end, Predicate pred) const
{
    assert(begin < end);

    const size_t linear_probe_threadhold = 16;
    size_t linear_probe_end = begin + linear_probe_threadhold;
    if (linear_probe_end > end)
        linear_probe_end = end;

    for (size_t pos = begin; pos < linear_probe_end; ++pos)
    {
        if (!pred(begin, pos))
            return pos;
    }

    size_t low = linear_probe_end;
    size_t high = end - 1;
    while (low <= high)
    {
        size_t mid = low + (high - low) / 2;
        if (pred(begin, mid))
            low = mid + 1;
        else
        {
            high = mid - 1;
            end = mid;
        }
    }
    return end;
}

std::pair<size_t, size_t> DistinctSortedChunkTransform::continueWithPrevRange(const size_t chunk_rows, IColumn::Filter & filter)
{
    /// prev_chunk_latest_key is empty on very first transform() call
    /// or first row doesn't match a key from previous transform()
    if (prev_chunk_latest_key.empty() || !isLatestKeyFromPrevChunk(0))
        return {0, 0};

    size_t output_rows = 0;
    const size_t range_end = getRangeEnd(0, chunk_rows, [&](size_t, size_t row_pos) { return isLatestKeyFromPrevChunk(row_pos); });
    if (other_columns.empty())
        std::fill(filter.begin(), filter.begin() + range_end, 0); /// skip rows already included in distinct on previous transform()
    else
    {
        constexpr bool clear_data = false;
        output_rows = ordinaryDistinctOnRange<clear_data>(filter, 0, range_end);
    }

    return {range_end, output_rows};
}

void DistinctSortedChunkTransform::transform(Chunk & chunk)
{
    const size_t chunk_rows = chunk.getNumRows();
    if (unlikely(0 == chunk_rows))
        return;

    Columns input_columns = chunk.detachColumns();
    /// split input columns into sorted and other("non-sorted") columns
    initChunkProcessing(input_columns);

    /// build filter:
    /// (1) find range with the same values in sorted columns -> [range_begin, range_end)
    /// (2) for found range
    ///     if there is no "non-sorted" columns: filter out all rows in range except first one
    ///     otherwise: apply ordinary distinct
    /// (3) repeat until chunk is processed
    IColumn::Filter filter(chunk_rows);
    auto [range_begin, output_rows] = continueWithPrevRange(chunk_rows, filter); /// try to process chuck as continuation of previous one
    size_t range_end = range_begin;
    while (range_end != chunk_rows)
    {
        // find new range [range_begin, range_end)
        range_end = getRangeEnd(range_begin, chunk_rows, [&](size_t key_pos, size_t row_pos) { return isKey(key_pos, row_pos); });

        // update filter for range
        if (other_columns.empty())
        {
            filter[range_begin] = 1;
            std::fill(filter.begin() + range_begin + 1, filter.begin() + range_end, 0);
            ++output_rows;
        }
        else
        {
            // ordinary distinct in range if there are "non-sorted" columns
            constexpr bool clear_data = true;
            output_rows += ordinaryDistinctOnRange<clear_data>(filter, range_begin, range_end);
        }

        // set where next range start
        range_begin = range_end;
    }
    /// if there is no any new rows in this chunk, just skip it
    if (!output_rows)
    {
        chunk.clear();
        return;
    }

    saveLatestKey(chunk_rows - 1);

    /// apply the built filter
    for (auto & input_column : input_columns)
        input_column = input_column->filter(filter, output_rows);

    chunk.setColumns(std::move(input_columns), output_rows);

    /// Update total output rows and check limits
    total_output_rows += output_rows;
    if ((limit_hint && total_output_rows >= limit_hint)
        || !output_size_limits.check(total_output_rows, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
    {
        stopReading();
    }
}

}
