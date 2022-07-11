#include <Processors/Transforms/DistinctSortedTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctSortedTransform::DistinctSortedTransform(
    const Block & header, SortDescription sort_description, const SizeLimits & set_size_limits_, UInt64 limit_hint_, const Names & columns)
    : ISimpleTransform(header, header, true)
    , description(std::move(sort_description))
    , columns_names(columns)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
{
}

void DistinctSortedTransform::transform(Chunk & chunk)
{
        const ColumnRawPtrs column_ptrs(getKeyColumns(chunk));
        if (column_ptrs.empty())
            return;

        ColumnRawPtrs clearing_hint_columns(getClearingColumns(chunk, column_ptrs));

        if (data.type == ClearableSetVariants::Type::EMPTY)
            data.init(ClearableSetVariants::chooseMethod(column_ptrs, key_sizes));

        const size_t rows = chunk.getNumRows();
        IColumn::Filter filter(rows);

        bool has_new_data = false;
        switch (data.type)
        {
            case ClearableSetVariants::Type::EMPTY:
                break;
    #define M(NAME) \
            case ClearableSetVariants::Type::NAME: \
                has_new_data = buildFilter(*data.NAME, column_ptrs, clearing_hint_columns, filter, rows, data); \
                break;
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
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
        prev_chunk.clearing_hint_columns = std::move(clearing_hint_columns);

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

ColumnRawPtrs DistinctSortedTransform::getKeyColumns(const Chunk & chunk) const
{
    size_t columns = columns_names.empty() ? chunk.getNumColumns() : columns_names.size();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        auto pos = i;
        if (!columns_names.empty())
            pos = input.getHeader().getPositionByName(columns_names[i]);

        const auto & column = chunk.getColumns()[pos];

        /// Ignore all constant columns.
        if (!isColumnConst(*column))
            column_ptrs.emplace_back(column.get());
    }

    return column_ptrs;
}

ColumnRawPtrs DistinctSortedTransform::getClearingColumns(const Chunk & chunk, const ColumnRawPtrs & key_columns) const
{
    ColumnRawPtrs clearing_hint_columns;
    clearing_hint_columns.reserve(description.size());
    for (const auto & sort_column_description : description)
    {
        const auto * sort_column_ptr = chunk.getColumns().at(sort_column_description.column_number).get();
        const auto it = std::find(key_columns.cbegin(), key_columns.cend(), sort_column_ptr);
        if (it != key_columns.cend()) /// if found in key_columns
            clearing_hint_columns.emplace_back(sort_column_ptr);
        else
            return clearing_hint_columns; /// We will use common prefix of sort description and requested DISTINCT key.
    }
    return clearing_hint_columns;
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
