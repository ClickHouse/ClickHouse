#include <Processors/Transforms/DistinctPrimaryKeyTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctPrimaryKeyTransform::DistinctPrimaryKeyTransform(
    const Block & header_,
    const SizeLimits & output_size_limits_,
    UInt64 limit_hint_,
    const SortDescription & sorted_columns_descr_,
    const Names & source_columns)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , output_size_limits(output_size_limits_)
    , sorted_columns_descr(sorted_columns_descr_)
{
    /// calculate sorted columns positions
    sorted_columns_pos.reserve(sorted_columns_descr.size());
    for (auto const & descr : sorted_columns_descr)
    {
        size_t pos = header_.getPositionByName(descr.column_name);
        sorted_columns_pos.emplace_back(pos);
    }

    /// calculate not-sorted columns positions
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
    current_key.reserve(sorted_columns.size());
}

void DistinctPrimaryKeyTransform::initChunkProcessing(const Columns & input_columns)
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

size_t DistinctPrimaryKeyTransform::ordinaryDistinctOnRange(IColumn::Filter & filter, size_t range_begin, size_t range_end)
{
    size_t count = 0;
    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
            // clang-format off
#define M(NAME) \
        case ClearableSetVariants::Type::NAME: \
            count = buildFilterForRange(*data.NAME, filter, range_begin, range_end, data); \
            break;

        APPLY_FOR_SET_VARIANTS(M)
#undef M
            // clang-format on
    }
    return count;
}

template <typename Method>
size_t DistinctPrimaryKeyTransform::buildFilterForRange(
    Method & method, IColumn::Filter & filter, size_t range_begin, size_t range_end, ClearableSetVariants & variants)
{
    typename Method::State state(other_columns, other_columns_sizes, nullptr);
    method.data.clear();

    size_t count = 0;
    for (size_t i = range_begin; i < range_end; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        /// emit the record if there is no such key in the current set, skip otherwise
        filter[i] = emplace_result.isInserted();
        if (filter[i])
            ++count;
    }
    return count;
}

void DistinctPrimaryKeyTransform::setCurrentKey(const size_t row_pos)
{
    current_key.clear();
    for (auto const & col : sorted_columns)
    {
        current_key.emplace_back(col->cloneEmpty());
        current_key.back()->insertFrom(*col, row_pos);
    }
}

bool DistinctPrimaryKeyTransform::isCurrentKey(const size_t row_pos)
{
    for (size_t i = 0; i < sorted_columns.size(); ++i)
    {
        const auto & sort_col_desc = sorted_columns_descr[i];
        int res = sort_col_desc.direction * current_key[i]->compareAt(0, row_pos, *sorted_columns[i], sort_col_desc.nulls_direction);
        if (res != 0)
            return false;
    }
    return true;
}

size_t DistinctPrimaryKeyTransform::getRangeEnd(size_t range_begin, size_t range_end)
{
    size_t low = range_begin;
    size_t high = range_end-1;
    while (low <= high)
    {
        size_t mid = low + (high - low) / 2;
        if (isCurrentKey(mid))
            low = mid + 1;
        else
        {
            high = mid - 1;
            range_end = mid;
        }
    }
    return range_end;
}

size_t DistinctPrimaryKeyTransform::getStartPosition(const size_t chunk_rows)
{
    if (!current_key.empty()) // current_key is empty on very first transform() call
    {
        if (other_columns.empty() && isCurrentKey(0))
            return getRangeEnd(0, chunk_rows);
    }
    return 0;
}

void DistinctPrimaryKeyTransform::transform(Chunk & chunk)
{
    const size_t chunk_rows = chunk.getNumRows();
    size_t output_rows = 0;
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
    size_t range_begin = getStartPosition(chunk_rows);
    if (range_begin > 0)
        std::fill(filter.begin(), filter.begin() + range_begin, 0); /// skip rows already included in distinct on previous transform()

    size_t range_end = range_begin;
    while (range_end != chunk_rows)
    {
        // set current key to find range
        setCurrentKey(range_begin);

        // find new range [range_begin, range_end)
        range_end = getRangeEnd(range_begin, chunk_rows);

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
            output_rows += ordinaryDistinctOnRange(filter, range_begin, range_end);
        }

        // set where next range start
        range_begin = range_end;
    }

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
