#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Columns/IColumn.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

static bool less(const MutableColumns & lhs, const Columns & rhs, size_t i, size_t j, const SortDescription & descr)
{
    size_t key_ind = 0;
    for (const auto & elem : descr)
    {
        size_t ind = elem.column_number;
        int res = elem.direction * lhs[key_ind++]->compareAt(i, j, *rhs[ind], elem.nulls_direction);
        if (res < 0)
            return true;
        else if (res > 0)
            return false;
    }
    return false;
}

DistinctSortedTransform::DistinctSortedTransform(
    const Block &header_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const SortDescription & sorted_columns_descr_,
    const Names & source_columns)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
    , sorted_columns_descr(sorted_columns_descr_)
{
    /// Calculate sorted columns positions
    sorted_columns_pos.reserve(sorted_columns_descr.size());
    for (auto & sorted_column_descr : sorted_columns_descr)
    {
        if (!sorted_column_descr.column_name.empty())
        {
            sorted_column_descr.column_number = header_.getPositionByName(sorted_column_descr.column_name);
            sorted_column_descr.column_name.clear();
        }
        sorted_columns_pos.emplace_back(sorted_column_descr.column_number);
    }

    /// Calculate not-sorted columns positions
    other_columns_pos.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
    {
        size_t pos = header_.getPositionByName(source_column);
        if (std::find(sorted_columns_pos.begin(), sorted_columns_pos.end(), pos) != sorted_columns_pos.end())
            continue;

        const auto & col = header_.getByPosition(pos).column;
        if (!(col && isColumnConst(*col)))
            other_columns_pos.emplace_back(pos);
    }
}


ColumnRawPtrs DistinctSortedTransform::getColumnsByPositions(const Columns & source_columns, const ColumnNumbers & positions)
{
    ColumnRawPtrs other_columns;
    other_columns.reserve(positions.size());
    for (size_t pos : positions)
        other_columns.emplace_back(source_columns[pos].get());
    return other_columns;
}


void DistinctSortedTransform::setCurrentKey(
    const ColumnRawPtrs & sorted_columns, IColumn::Filter & filter, size_t key_pos)
{
    current_key.clear();
    current_key.resize(sorted_columns.size());
    for (size_t i = 0; i < sorted_columns.size(); ++i)
    {
        current_key[i] = sorted_columns[i]->cloneEmpty();
        current_key[i]->insertFrom(*sorted_columns[i], key_pos);
    }
    filter[key_pos] = 1;
}


void DistinctSortedTransform::executeOnInterval(
    const ColumnRawPtrs & other_columns, IColumn::Filter & filter, size_t key_begin, size_t key_end)
{
    if (data.type == ClearableSetVariants::Type::EMPTY)
        data.init(ClearableSetVariants::chooseMethod(other_columns, other_columns_sizes));

    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
#define M(NAME) \
            case ClearableSetVariants::Type::NAME: \
                buildFilterForInterval(*data.NAME, other_columns, filter, key_begin, key_end, data); \
                break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }
}


template <typename Method>
void DistinctSortedTransform::buildFilterForInterval(
    Method & method,
    const ColumnRawPtrs & other_columns,
    IColumn::Filter & filter,
    size_t key_begin,
    size_t key_end,
    ClearableSetVariants & variants)
{
    typename Method::State state(other_columns, other_columns_sizes, nullptr);

    if (is_new_key)
        method.data.clear();

    for (size_t i = key_begin; i < key_end; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = emplace_result.isInserted();
        if (filter[i])
            ++cur_block_rows;
    }
}

void DistinctSortedTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    total_rows += cur_block_rows;
    cur_block_rows = 0;

    /// Stop reading if we already reach the limit.
    if (limit_hint && total_rows >= limit_hint)
    {
        stopReading();
        return;
    }
    Columns source_columns = chunk.detachColumns();
    ColumnRawPtrs sorted_columns = getColumnsByPositions(source_columns, sorted_columns_pos);
    ColumnRawPtrs other_columns = getColumnsByPositions(source_columns, other_columns_pos);

    IColumn::Filter filter(num_rows);
    size_t key_end = 0;
    size_t key_begin = 0;

    /// Values used for binary search only
    ssize_t mid = 0;
    ssize_t high = 0;
    ssize_t low = -1;

    while (key_end != num_rows)
    {
        if (is_new_key)
            setCurrentKey(sorted_columns, filter, key_begin);

        high = num_rows;
        while (high - low > 1)
        {
            mid = (low + high) / 2;
            if (!less(current_key, source_columns, 0, mid, sorted_columns_descr))
                low = mid;
            else
                high = mid;
        }
        key_end = high;
        if (key_begin != key_end)
        {
            executeOnInterval(other_columns, filter, key_begin, key_end);
            if (!set_size_limits.check(total_rows, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                return;
        }
        low = key_begin = key_end;
        is_new_key = (key_begin != num_rows);
    }
    for (auto & source_column : source_columns)
        source_column = source_column->filter(filter, -1);

    chunk.setColumns(std::move(source_columns), cur_block_rows);
}

}

