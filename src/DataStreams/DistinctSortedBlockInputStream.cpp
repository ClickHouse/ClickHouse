#include <DataStreams/DistinctSortedBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctSortedBlockInputStream::DistinctSortedBlockInputStream(
    const BlockInputStreamPtr & input, SortDescription sort_description, const SizeLimits & set_size_limits_, UInt64 limit_hint_, const Names & columns)
    : description(std::move(sort_description))
    , columns_names(columns)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
{
    children.push_back(input);
}

Block DistinctSortedBlockInputStream::readImpl()
{
    /// Execute until end of stream or until
    /// a block with some new records will be gotten.
    for (;;)
    {
        /// Stop reading if we already reached the limit.
        if (limit_hint && data.getTotalRowCount() >= limit_hint)
            return Block();

        Block block = children.back()->read();
        if (!block)
            return Block();

        const ColumnRawPtrs column_ptrs(getKeyColumns(block));
        if (column_ptrs.empty())
            return block;

        const ColumnRawPtrs clearing_hint_columns(getClearingColumns(block, column_ptrs));

        if (data.type == ClearableSetVariants::Type::EMPTY)
            data.init(ClearableSetVariants::chooseMethod(column_ptrs, key_sizes));

        const size_t rows = block.rows();
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
            continue;

        if (!set_size_limits.check(data.getTotalRowCount(), data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
            return {};

        prev_block.block = block;
        prev_block.clearing_hint_columns = std::move(clearing_hint_columns);

        size_t all_columns = block.columns();
        for (size_t i = 0; i < all_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);

        return block;
    }
}


template <typename Method>
bool DistinctSortedBlockInputStream::buildFilter(
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
    if (!clearing_hint_columns.empty() && !prev_block.clearing_hint_columns.empty()
        && !rowsEqual(clearing_hint_columns, 0, prev_block.clearing_hint_columns, prev_block.block.rows() - 1))
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

ColumnRawPtrs DistinctSortedBlockInputStream::getKeyColumns(const Block & block) const
{
    size_t columns = columns_names.empty() ? block.columns() : columns_names.size();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & column = columns_names.empty()
            ? block.safeGetByPosition(i).column
            : block.getByName(columns_names[i]).column;

        /// Ignore all constant columns.
        if (!isColumnConst(*column))
            column_ptrs.emplace_back(column.get());
    }

    return column_ptrs;
}

ColumnRawPtrs DistinctSortedBlockInputStream::getClearingColumns(const Block & block, const ColumnRawPtrs & key_columns) const
{
    ColumnRawPtrs clearing_hint_columns;
    clearing_hint_columns.reserve(description.size());
    for (const auto & sort_column_description : description)
    {
        const auto * sort_column_ptr = block.safeGetByPosition(sort_column_description.column_number).column.get();
        const auto it = std::find(key_columns.cbegin(), key_columns.cend(), sort_column_ptr);
        if (it != key_columns.cend()) /// if found in key_columns
            clearing_hint_columns.emplace_back(sort_column_ptr);
        else
            return clearing_hint_columns; /// We will use common prefix of sort description and requested DISTINCT key.
    }
    return clearing_hint_columns;
}

bool DistinctSortedBlockInputStream::rowsEqual(const ColumnRawPtrs & lhs, size_t n, const ColumnRawPtrs & rhs, size_t m)
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
