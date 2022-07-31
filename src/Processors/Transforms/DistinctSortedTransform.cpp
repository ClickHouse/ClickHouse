#include <Processors/Transforms/DistinctSortedTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

static void handleAllColumnsConst(Chunk & chunk)
{
    const size_t rows = chunk.getNumRows();
    IColumn::Filter filter(rows);

    Chunk res_chunk;
    std::fill(filter.begin(), filter.end(), 0);
    filter[0] = 1;
    for (const auto & column : chunk.getColumns())
        res_chunk.addColumn(column->filter(filter, -1));

    chunk = std::move(res_chunk);
}

DistinctSortedTransform::DistinctSortedTransform(
    Block header_, SortDescription sort_description, const SizeLimits & set_size_limits_, UInt64 limit_hint_, const Names & columns)
    : ISimpleTransform(header_, header_, true)
    , header(std::move(header_))
    , description(std::move(sort_description))
    , column_names(columns)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
{
    /// pre-calculate column positions to use during chunk transformation
    const size_t num_columns = column_names.empty() ? header.columns() : column_names.size();
    column_positions.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto pos = column_names.empty() ? i : header.getPositionByName(column_names[i]);
        const auto & column = header.getByPosition(pos).column;
        if (column && !isColumnConst(*column))
        {
            column_positions.emplace_back(pos);
            all_columns_const = false;
        }
    }
    column_ptrs.reserve(column_positions.size());

    /// pre-calculate DISTINCT column positions which form sort prefix of sort description
    sort_prefix_positions.reserve(description.size());
    for (const auto & column_sort_descr : description)
    {
        /// check if there is such column in header
        if (!header.has(column_sort_descr.column_name))
            break;

        /// check if sorted column position matches any DISTINCT column
        const auto pos = header.getPositionByName(column_sort_descr.column_name);
        if (std::find(begin(column_positions), end(column_positions), pos) == column_positions.end())
            break;

        sort_prefix_positions.emplace_back(pos);
    }
    sort_prefix_columns.reserve(sort_prefix_positions.size());
}

void DistinctSortedTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// special case - all column constant
    if (unlikely(all_columns_const))
    {
        handleAllColumnsConst(chunk);
        stopReading();
        return;
    }

    /// get DISTINCT columns from chunk
    column_ptrs.clear();
    for (const auto pos : column_positions)
    {
        const auto & column = chunk.getColumns()[pos];
        column_ptrs.emplace_back(column.get());
    }

    /// get DISTINCT columns from chunk which form sort prefix of sort description
    sort_prefix_columns.clear();
    for (const auto pos : sort_prefix_positions)
    {
        const auto & column = chunk.getColumns()[pos];
        sort_prefix_columns.emplace_back(column.get());
    }

    if (data.type == ClearableSetVariants::Type::EMPTY)
        data.init(ClearableSetVariants::chooseMethod(column_ptrs, key_sizes));

    const size_t rows = chunk.getNumRows();
    IColumn::Filter filter(rows);

    bool has_new_data = false;
    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
            // clang-format off
#define M(NAME) \
        case ClearableSetVariants::Type::NAME: \
            has_new_data = buildFilter(*data.NAME, column_ptrs, sort_prefix_columns, filter, rows, data); \
            break;

        APPLY_FOR_SET_VARIANTS(M)
#undef M
            // clang-format on
    }

    /// Just go to the next block if there isn't any new record in the current one.
    if (!has_new_data)
    {
        chunk.clear();
        return;
    }

    if (!set_size_limits.check(data.getTotalRowCount(), data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
    {
        stopReading();
        chunk.clear();
        return;
    }

    /// Stop reading if we already reached the limit.
    if (limit_hint && data.getTotalRowCount() >= limit_hint)
        stopReading();

    prev_chunk.chunk = std::move(chunk);
    prev_chunk.clearing_hint_columns = std::move(sort_prefix_columns);

    size_t all_columns = prev_chunk.chunk.getNumColumns();
    Chunk res_chunk;
    for (size_t i = 0; i < all_columns; ++i)
        res_chunk.addColumn(prev_chunk.chunk.getColumns().at(i)->filter(filter, -1));

    chunk = std::move(res_chunk);
}


template <typename Method>
bool DistinctSortedTransform::buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    const ColumnRawPtrs & clearing_hint_columns,
    IColumn::Filter & filter,
    size_t rows,
    ClearableSetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    /// Compare last row of previous block and first row of current block,
    /// If rows not equal, we can clear HashSet,
    /// If clearing_hint_columns is empty, we CAN'T clear HashSet.
    if (!clearing_hint_columns.empty() && !prev_chunk.clearing_hint_columns.empty()
        && !rowsEqual(clearing_hint_columns, 0, prev_chunk.clearing_hint_columns, prev_chunk.chunk.getNumRows() - 1))
    {
        method.data.clear();
    }

    bool has_new_data = false;
    for (size_t i = 0; i < rows; ++i)
    {
        /// Compare i-th row and i-1-th row,
        /// If rows are not equal, we can clear HashSet,
        /// If clearing_hint_columns is empty, we CAN'T clear HashSet.
        if (i > 0 && !clearing_hint_columns.empty() && !rowsEqual(clearing_hint_columns, i, clearing_hint_columns, i - 1))
            method.data.clear();

        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        if (emplace_result.isInserted())
            has_new_data = true;

        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = emplace_result.isInserted();
    }
    return has_new_data;
}

bool DistinctSortedTransform::rowsEqual(const ColumnRawPtrs & lhs, size_t n, const ColumnRawPtrs & rhs, size_t m)
{
    for (size_t column_index = 0, num_columns = lhs.size(); column_index < num_columns; ++column_index)
    {
        const auto & lhs_column = *lhs[column_index];
        const auto & rhs_column = *rhs[column_index];
        if (lhs_column.compareAt(n, m, rhs_column, 0) != 0) /// not equal
            return false;
    }
    return true;
}

}
